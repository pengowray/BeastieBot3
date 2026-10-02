#!/usr/bin/env bash
# Uploads a site database (built by `site build-db`) to the server and makes it the live database.
#
#   deploy/oracle/deploy-db.sh [PATH]
#
# PATH defaults to SITE_DB in deploy.env, then to [Datastore] site_sqlite in BeastieBot3/paths.ini
# (or the file named by PATHS_INI).
#
# Steps: check the file (SQLite, meta table, PRAGMA quick_check); copy it to rollback journal mode
# when it is in WAL mode; compress it with zstd (gzip when zstd is missing); upload it to
# /srv/beastie/incoming with rsync --partial, so an interrupted upload resumes when you run the
# script again; on the server, decompress it, compare its SHA-256 with the local file, move it into
# place and restart the site. When /healthz passes, the database it replaced becomes
# site.sqlite.prev. When /healthz fails, the server puts the replaced database back and keeps the
# new one as site.sqlite.failed.
#
# Do not run it while `site build-db` is still writing the file.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "${BASH_SOURCE[0]}"
}

# Path of the site database: the argument, else SITE_DB, else paths.ini.
resolve_db_path() {
    local arg="${1:-}"
    if [[ -n "$arg" ]]; then
        printf '%s' "$arg"
        return 0
    fi
    if [[ -n "${SITE_DB:-}" ]]; then
        printf '%s' "$SITE_DB"
        return 0
    fi
    local ini="${PATHS_INI:-$REPO_ROOT/BeastieBot3/paths.ini}"
    [[ -f "$ini" ]] || die "No database path given, SITE_DB is not set, and $ini does not exist."
    python3 - "$ini" <<'PY'
import configparser, os, sys
ini = sys.argv[1]
parser = configparser.ConfigParser(interpolation=None, strict=False)
parser.read(ini, encoding="utf-8")
value = parser.get("Datastore", "site_sqlite", fallback="").strip()
if not value:
    sys.exit(f"Error: {ini} has no [Datastore] site_sqlite line. Pass the database path as an argument.")
value = os.path.expanduser(value)
if not os.path.isabs(value):
    # The CLI resolves relative values against its bin folder; this is only a best guess.
    value = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(ini)), value))
print(value, end="")
PY
}

# Checks the database and prints "<file to upload>\t<sha256>\t<bytes>". The file to upload is the
# database itself, or a copy in rollback journal mode when the database is in WAL mode.
check_and_prepare() {
    python3 - "$1" "$WORK_DIR" "$(expected_schema_version)" <<'PY'
import hashlib, os, sqlite3, sys, urllib.parse

db, work_dir, expected = sys.argv[1], sys.argv[2], sys.argv[3]

def fail(message):
    print(f"Error: {message}", file=sys.stderr)
    sys.exit(1)

def note(message):
    print(message, file=sys.stderr)

def connect_ro(path):
    return sqlite3.connect("file:" + urllib.parse.quote(os.path.abspath(path)) + "?mode=ro", uri=True)

if not os.path.isfile(db):
    fail(f"Database not found: {db}")
with open(db, "rb") as f:
    header = f.read(100)
if not header.startswith(b"SQLite format 3\x00"):
    fail(f"Not a SQLite database: {db}")

wal_mode = header[18] == 2 or header[19] == 2
wal_file = db + "-wal"
wal_has_data = os.path.exists(wal_file) and os.path.getsize(wal_file) > 0

con = connect_ro(db)
if con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'").fetchone() is None:
    fail(f"{db} has no meta table. Build the site database with: "
         "dotnet run --project BeastieBot3/BeastieBot3.csproj -- site build-db")
meta = dict(con.execute("SELECT key, value FROM meta"))
version = meta.get("schema_version", "")
note(f"Database:       {db} ({os.path.getsize(db) / 1e6:,.0f} MB)")
note(f"IUCN release:   {meta.get('iucn_release', '?')}")
note(f"Built (UTC):    {meta.get('built_at_utc', '?')}")
note(f"Schema version: {version or '?'}")
note(f"Taxa:           {meta.get('taxon_count', '?')}")
if not version:
    fail("The meta table has no schema_version. The site would refuse this database.")
if expected and version != expected:
    note(f"Warning: this checkout's site expects schema version {expected}, and the database has version {version}.\n"
         "If the server runs an app built from this checkout, the health check will fail and the previous database will be put back.")

note("Running PRAGMA quick_check ...")
result = con.execute("PRAGMA quick_check").fetchone()[0]
if result != "ok":
    fail(f"PRAGMA quick_check failed: {result}")

source = db
if wal_mode or wal_has_data:
    snapshot = os.path.join(work_dir, "site-snapshot.sqlite")
    if os.path.exists(snapshot):
        os.remove(snapshot)
    note("The database is in WAL mode, which the site cannot open from a read-only folder.")
    note(f"Copying it in rollback journal mode to {snapshot} ...")
    con.execute("VACUUM INTO ?", (snapshot,))
    source = snapshot
con.close()

digest = hashlib.sha256()
with open(source, "rb") as f:
    for block in iter(lambda: f.read(1 << 20), b""):
        digest.update(block)
print(f"{source}\t{digest.hexdigest()}\t{os.path.getsize(source)}", end="")
PY
}

# Compresses the file into WORK_DIR, unless an earlier run already did, and prints the path.
compress() {
    local source="$1" sha="$2" base out
    base="$WORK_DIR/site-${sha:0:12}.sqlite"
    if command -v zstd >/dev/null 2>&1; then
        out="$base.zst"
        if [[ ! -f "$out" ]]; then
            say "Compressing with zstd (level $ZSTD_LEVEL) ..." >&2
            zstd -q -T0 "-$ZSTD_LEVEL" -f -o "$out.part" "$source"
            mv -f "$out.part" "$out"
        fi
    else
        out="$base.gz"
        if [[ ! -f "$out" ]]; then
            say "Compressing with gzip (zstd is not installed) ..." >&2
            gzip -c -6 "$source" > "$out.part"
            mv -f "$out.part" "$out"
        fi
    fi
    printf '%s' "$out"
}

main() {
    case "${1:-}" in
        -h|--help) usage; return 0 ;;
        -*) die "Unknown option: $1" ;;
    esac
    load_env
    command -v python3 >/dev/null 2>&1 || die "python3 is needed to check the database."

    step "Checking the database"
    local db prepared source sha size
    db="$(resolve_db_path "${1:-}")"
    prepared="$(check_and_prepare "$db")"
    IFS=$'\t' read -r source sha size <<< "$prepared"
    say "SHA-256:        $sha"

    step "Comparing with the live database on $HOST"
    local live_sha
    live_sha="$(remote_task live-db-sha)"
    if [[ "$live_sha" == "$sha" ]]; then
        say "The server already has this database. Nothing to do."
        return 0
    fi
    if [[ -n "$live_sha" ]]; then
        say "The server has a different database. Deploying this one."
    else
        say "The server has no database yet."
    fi

    step "Compressing"
    local compressed name
    compressed="$(compress "$source" "$sha")"
    name="$(basename "$compressed")"
    say "$compressed ($(du -h "$compressed" | cut -f 1))"

    step "Uploading to /srv/beastie/incoming/$name"
    rsync_to_server --partial --chmod=F644 -h --info=progress2 \
        "$compressed" "$TARGET:/srv/beastie/incoming/$name"

    step "Installing on the server"
    local rc=0
    remote_task install-db "$name" "$sha" "$size" || rc=$?
    case "$rc" in
        0) check_public_health loopback-ok ;;
        3) ;;
        *) exit "$rc" ;;
    esac
    rm -f -- "$WORK_DIR"/site-*.sqlite.zst "$WORK_DIR"/site-*.sqlite.gz "$WORK_DIR/site-snapshot.sqlite"
}

main "$@"
