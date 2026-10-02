#!/usr/bin/env bash
# Tasks that run on the server as root, for the BeastieBot3 public site.
#
# The deploy scripts send this file over SSH each time they run a task
# (ssh host 'sudo bash -s -- <task> [arguments]' < server-tasks.sh), so the server always runs the
# copy that matches the scripts on your computer. setup-server.sh also installs a copy as
# /usr/local/sbin/beastie-site, for use when you are logged in to the server:
#
#   sudo beastie-site status        app release, service state, health check, databases, disk space
#   sudo beastie-site rollback-db   swap site.sqlite and site.sqlite.prev, then restart
#   sudo beastie-site rollback-app  switch to the release before the current one, then restart
#
# Nothing in this file may read standard input: when bash reads the script over SSH, standard
# input is the script itself. Everything is inside functions and main runs on the last line, so
# by then bash has read the whole file.
set -euo pipefail

BASE=/srv/beastie
APP_LINK=$BASE/app
RELEASES=$BASE/releases
INCOMING=$BASE/incoming
DATA=$BASE/data
DB=$DATA/site.sqlite
SERVICE=beastie-site
HEALTH_URL=http://127.0.0.1:5080/healthz
KEEP_RELEASES=3

say()  { printf '%s\n' "$*"; }
warn() { printf 'Warning: %s\n' "$*" >&2; }
die()  { printf 'Error: %s\n' "$*" >&2; exit 1; }

usage() {
    cat <<'EOF'
Usage: beastie-site <task>

Tasks for use on the server:
  status         Show the app release, service state, health check, databases and disk space.
  rollback-db    Swap site.sqlite and site.sqlite.prev, then restart the site.
                 Running it a second time puts the newer database back.
  rollback-app   Switch to the release before the current one, then restart the site.

Tasks the deploy scripts run:
  app-info, activate-app <release>, live-db-sha, install-db <file> <sha256> <bytes>
EOF
}

# --- Health ---------------------------------------------------------------------------------

# A failed start does not stop the task: the health check that follows reports the failure with
# the service log, and systemd tries the start again every 5 seconds (Restart=always).
restart_service() {
    if ! systemctl restart "$SERVICE" </dev/null; then
        warn "systemctl restart $SERVICE reported an error. Waiting for the health check anyway."
    fi
}

# Polls the site's /healthz on the loopback port until it answers 200 or HEALTH_TIMEOUT seconds pass.
wait_healthy() {
    local timeout="${HEALTH_TIMEOUT:-60}" waited=0
    while (( waited < timeout )); do
        if curl -fsS --max-time 5 -o /dev/null "$HEALTH_URL" 2>/dev/null; then
            return 0
        fi
        sleep 2
        waited=$(( waited + 2 ))
    done
    return 1
}

# One line: "ok", or what /healthz answered, or why it could not be reached.
health_summary() {
    local out code body
    if out="$(curl -sS --max-time 5 -w '\n%{http_code}' "$HEALTH_URL" 2>&1)"; then
        code="${out##*$'\n'}"
        body="${out%$'\n'*}"
        if [[ "$code" == 200 ]]; then
            say "ok"
        else
            say "failed: HTTP $code${body:+, response: ${body:0:300}}"
        fi
    else
        say "failed: ${out:0:300}"
    fi
}

report_unhealthy() {
    warn "The site did not pass its health check within ${HEALTH_TIMEOUT:-60} seconds."
    say "Health check: $(health_summary)"
    say ""
    say "Service state:"
    systemctl status "$SERVICE" --no-pager --lines=0 </dev/null || true
    say ""
    say "Last 30 lines of the service log:"
    journalctl -u "$SERVICE" -n 30 --no-pager </dev/null || true
}

# --- Databases -------------------------------------------------------------------------------

# Prints one line about a site database: IUCN release, build time, schema version, taxon count, size.
db_info() {
    local path="$1"
    if [[ ! -f "$path" ]]; then
        say "none"
        return 0
    fi
    python3 - "$path" <<'PY'
import os, sqlite3, sys
path = sys.argv[1]
size_mb = os.path.getsize(path) / 1e6
try:
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    meta = dict(con.execute("SELECT key, value FROM meta"))
    con.close()
except Exception as e:
    print(f"cannot read the meta table ({e}), {size_mb:,.0f} MB")
    sys.exit(0)
print("IUCN release {r}, built {b}, schema version {v}, {t} taxa, {s:,.0f} MB".format(
    r=meta.get("iucn_release", "?"), b=meta.get("built_at_utc", "?"),
    v=meta.get("schema_version", "?"), t=meta.get("taxon_count", "?"), s=size_mb))
PY
}

db_schema_version() {
    python3 - "$1" <<'PY'
import sqlite3, sys
try:
    con = sqlite3.connect(f"file:{sys.argv[1]}?mode=ro", uri=True)
    row = con.execute("SELECT value FROM meta WHERE key = 'schema_version'").fetchone()
    print(row[0] if row else "")
except Exception:
    print("")
PY
}

# Checks that a file is a SQLite database in rollback journal mode with a meta table. The site
# opens the database read-only in a folder it cannot write to, which works only when the file is
# not in WAL mode.
check_db_file() {
    python3 - "$1" <<'PY'
import sqlite3, sys
path = sys.argv[1]
with open(path, "rb") as f:
    header = f.read(100)
if not header.startswith(b"SQLite format 3\x00"):
    sys.exit("not a SQLite database")
if header[18] == 2 or header[19] == 2:
    sys.exit("the database is in WAL mode")
con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
if con.execute("SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'meta'").fetchone() is None:
    sys.exit("the database has no meta table")
PY
}

live_db_sha() {
    if [[ -f "$DB" ]]; then
        sha256sum "$DB" | cut -d ' ' -f 1
    fi
}

app_deployed() {
    [[ -x "$APP_LINK/BeastieBot3.Site" ]]
}

# Decompresses an uploaded database from /srv/beastie/incoming, checks it, moves it into place and
# restarts the site. When the health check passes, the database it replaced becomes
# site.sqlite.prev; when it fails, the replaced database is put back and the new one is kept as
# site.sqlite.failed.
# Exit codes: 0 live and healthy (or already live), 2 health check failed, 3 installed but no app
# is deployed yet.
install_db() {
    local name="${1:-}" sha="${2:-}" size="${3:-}"
    [[ "$name" =~ ^site-[0-9a-f]{12}\.sqlite\.(zst|gz)$ ]] || die "Unexpected upload file name: $name"
    [[ "$sha" =~ ^[0-9a-f]{64}$ ]] || die "Expected a SHA-256 checksum, got: $sha"
    [[ "$size" =~ ^[0-9]+$ ]] || die "Expected a size in bytes, got: $size"
    local upload="$INCOMING/$name" new="$DATA/site.sqlite.new"
    [[ -f "$upload" ]] || die "Uploaded file not found: $upload"

    if [[ -f "$DB" && "$(live_db_sha)" == "$sha" ]]; then
        rm -f -- "$upload"
        say "This database is already the live database. Nothing changed."
        return 0
    fi

    local avail needed
    avail="$(df --output=avail -B1 "$DATA" | tail -n 1 | tr -d ' ')"
    needed=$(( size + size / 10 + 100000000 ))
    (( avail > needed )) || die "Not enough free disk space in $DATA: need $(( needed / 1000000 )) MB, have $(( avail / 1000000 )) MB."

    say "Decompressing $name ..."
    rm -f -- "$new"
    case "$name" in
        *.zst)
            command -v zstd >/dev/null || die "zstd is not installed on the server. Run setup-server.sh again."
            zstd -d -q -f -o "$new" "$upload" ;;
        *.gz)
            gzip -dc "$upload" > "$new" ;;
    esac

    local actual reason
    actual="$(sha256sum "$new" | cut -d ' ' -f 1)"
    if [[ "$actual" != "$sha" ]]; then
        rm -f -- "$new"
        die "Checksum mismatch after decompressing $name: expected $sha, got $actual. Run deploy-db.sh again."
    fi
    if ! reason="$(check_db_file "$new" 2>&1)"; then
        rm -f -- "$new"
        die "The uploaded database cannot be used: $reason"
    fi
    chown root:root "$new"
    chmod 644 "$new"
    sync "$new"

    # The live file is replaced in one rename, so site.sqlite exists at every moment. The running
    # site keeps reading the old file until it restarts. The old live file waits as
    # site.sqlite.outgoing and becomes site.sqlite.prev only when the new one passes the health
    # check, so a failed deploy leaves site.sqlite.prev as it was.
    local outgoing="$DB.outgoing" had_live=no
    rm -f -- "$outgoing"
    if [[ -f "$DB" ]]; then
        ln "$DB" "$outgoing"
        had_live=yes
    fi
    mv -f "$new" "$DB"
    rm -f -- "$upload"
    say "Installed the new database as $DB."
    say "  New:      $(db_info "$DB")"
    say "  Replaced: $(db_info "$outgoing")"

    if ! app_deployed; then
        if [[ "$had_live" == yes ]]; then
            mv -f "$outgoing" "$DB.prev"
        fi
        say "The app is not deployed yet, so the site was not restarted. Deploy it with deploy-app.sh."
        exit 3
    fi

    say "Restarting $SERVICE ..."
    restart_service
    if wait_healthy; then
        rm -f -- "$DB.failed"
        say "Health check passed: $HEALTH_URL"
        if [[ "$had_live" == yes ]]; then
            mv -f "$outgoing" "$DB.prev"
            say "The replaced database is kept as $DB.prev."
            say "To go back to it: deploy/oracle/rollback-db.sh (on the server: sudo beastie-site rollback-db)"
        fi
        return 0
    fi

    report_unhealthy
    if [[ "$had_live" == no ]]; then
        warn "There was no database before this one, so the new database stays in place."
        exit 2
    fi
    say ""
    say "Putting back the database that was live before. The new one is kept as $DB.failed."
    ln -f "$DB" "$DB.failed"
    mv -f "$outgoing" "$DB"
    restart_service
    if wait_healthy; then
        say "The database that was live before is live again, and the health check passed."
    else
        warn "The site also fails its health check with the database that was live before, so the cause is probably the app or the server, not the new database."
        say "Health check: $(health_summary)"
    fi
    exit 2
}

# Swaps site.sqlite and site.sqlite.prev and restarts the site. Running it twice restores the
# original order.
rollback_db() {
    [[ -f "$DB.prev" ]] || die "No previous database: $DB.prev does not exist."
    ln -f "$DB" "$DB.swap"
    mv -f "$DB.prev" "$DB"
    mv -f "$DB.swap" "$DB.prev"
    say "Swapped the live and previous databases."
    say "  Live now:     $(db_info "$DB")"
    say "  Previous now: $(db_info "$DB.prev")"
    if ! app_deployed; then
        say "The app is not deployed yet, so the site was not restarted."
        exit 3
    fi
    say "Restarting $SERVICE ..."
    restart_service
    if wait_healthy; then
        say "Health check passed: $HEALTH_URL"
    else
        report_unhealthy
        say ""
        say "To put the other database back, run the rollback again."
        exit 2
    fi
}

# --- App releases ----------------------------------------------------------------------------

valid_release_id() {
    [[ "$1" =~ ^[0-9]{8}-[0-9]{6}(-[A-Za-z0-9._-]+)?$ ]]
}

current_release() {
    if [[ -L "$APP_LINK" ]]; then
        basename "$(readlink "$APP_LINK")"
    fi
}

# Release folder names, oldest first. The names start with the UTC build time, so they sort by time.
list_releases() {
    local dir name
    for dir in "$RELEASES"/*/; do
        [[ -d "$dir" ]] || continue
        name="$(basename "$dir")"
        if valid_release_id "$name"; then
            printf '%s\n' "$name"
        fi
    done | LC_ALL=C sort
}

# Points /srv/beastie/app at a release in one rename.
switch_app() {
    if [[ -e "$APP_LINK" && ! -L "$APP_LINK" ]]; then
        die "$APP_LINK exists and is not a symbolic link. Move it away, then deploy again."
    fi
    ln -sfn "releases/$1" "$BASE/app.new"
    mv -Tf "$BASE/app.new" "$APP_LINK"
}

# Deletes all but the newest KEEP_RELEASES releases. The current release is always kept.
prune_releases() {
    local current name i
    current="$(current_release)"
    local -a all
    mapfile -t all < <(list_releases)
    local remove_count=$(( ${#all[@]} - KEEP_RELEASES ))
    for (( i = 0; i < remove_count; i++ )); do
        name="${all[$i]}"
        [[ "$name" == "$current" ]] && continue
        rm -rf -- "${RELEASES:?}/$name"
        say "Deleted old release $name."
    done
}

app_info() {
    say "arch=$(uname -m)"
    say "current_release=$(current_release)"
    if [[ -f "$DB" ]]; then
        say "db_present=yes"
        say "db_schema_version=$(db_schema_version "$DB")"
    else
        say "db_present=no"
    fi
}

after_switch_restart() {
    if [[ ! -f "$DB" ]]; then
        restart_service
        say "No database at $DB yet, so the site cannot pass its health check. Deploy one with deploy-db.sh."
        exit 3
    fi
    say "Restarting $SERVICE ..."
    restart_service
    if wait_healthy; then
        say "Health check passed: $HEALTH_URL"
    else
        report_unhealthy
        exit 2
    fi
}

activate_app() {
    local id="${1:-}"
    valid_release_id "$id" || die "Not a release name: $id"
    [[ -x "$RELEASES/$id/BeastieBot3.Site" ]] || die "Release $id has no executable BeastieBot3.Site in $RELEASES/$id"
    local previous
    previous="$(current_release)"
    switch_app "$id"
    say "The site now runs release $id (before: ${previous:-none})."
    prune_releases
    after_switch_restart
}

rollback_app() {
    local current previous="" name
    current="$(current_release)"
    [[ -n "$current" ]] || die "No release is active: $APP_LINK is not a symbolic link."
    while IFS= read -r name; do
        if [[ "$name" < "$current" ]]; then
            previous="$name"
        fi
    done < <(list_releases)
    [[ -n "$previous" ]] || die "There is no release older than $current in $RELEASES."
    switch_app "$previous"
    say "The site now runs release $previous (before: $current)."
    after_switch_restart
}

# --- Status ----------------------------------------------------------------------------------

show_status() {
    local releases
    releases="$(list_releases | paste -sd ' ' -)"
    say "App release:       $(current_release || true)"
    say "Releases on disk:  ${releases:-none}"
    say "Service:           $(systemctl is-active "$SERVICE" </dev/null || true)"
    say "Health check:      $(health_summary)"
    say "Live database:     $(db_info "$DB")"
    say "Previous database: $(db_info "$DB.prev")"
    if [[ -f "$DB.failed" ]]; then
        say "Failed database:   $(db_info "$DB.failed")"
    fi
    say "Caddy:             $(systemctl is-active caddy </dev/null || true)"
    say "Free disk space:   $(df -h --output=avail "$BASE" | tail -n 1 | tr -d ' ')"
    if [[ -f /var/run/reboot-required ]]; then
        say "Reboot required:   yes (sudo reboot)"
    fi
}

main() {
    local task="${1:-}"
    shift || true
    case "$task" in
        -h|--help|help|"") usage; return 0 ;;
    esac
    [[ $EUID -eq 0 ]] || die "Run this as root: sudo beastie-site $task"
    case "$task" in
        status)       show_status ;;
        rollback-db)  rollback_db ;;
        rollback-app) rollback_app ;;
        app-info)     app_info ;;
        activate-app) activate_app "$@" ;;
        live-db-sha)  live_db_sha ;;
        install-db)   install_db "$@" ;;
        *) usage >&2; exit 1 ;;
    esac
}

main "$@"
