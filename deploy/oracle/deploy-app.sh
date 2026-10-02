#!/usr/bin/env bash
# Builds BeastieBot3.Site for the server, uploads it as a new release and switches the site to it.
#
#   deploy/oracle/deploy-app.sh              build, upload, switch, restart and check /healthz
#   deploy/oracle/deploy-app.sh --rollback   switch back to the previous release and restart
#
# Each release is a folder /srv/beastie/releases/<UTC build time>-<git commit>, and
# /srv/beastie/app is a symbolic link to the current one. The server keeps the newest 3 releases.
# Files that are the same as in the current release are hard-linked instead of uploaded again.
#
# When the health check fails, this script does not switch back by itself, because a new app and a
# new database sometimes have to go out one after the other (see the README). It prints the
# rollback command instead.
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

usage() {
    awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "${BASH_SOURCE[0]}"
}

rollback() {
    step "Switching $DOMAIN back to the previous release"
    local rc=0
    remote_task rollback-app || rc=$?
    case "$rc" in
        0) check_public_health loopback-ok ;;
        3) exit 3 ;;
        *) exit "$rc" ;;
    esac
}

release_id() {
    local commit=nogit
    if git -C "$REPO_ROOT" rev-parse --short HEAD >/dev/null 2>&1; then
        commit="$(git -C "$REPO_ROOT" rev-parse --short HEAD)"
        if [[ -n "$(git -C "$REPO_ROOT" status --porcelain --untracked-files=no)" ]]; then
            commit+="-dirty"
        fi
    fi
    printf '%s-%s' "$(date -u +%Y%m%d-%H%M%S)" "$commit"
}

deploy() {
    load_env
    find_dotnet

    step "Checking the server"
    local info server_arch="" current="" db_present="" db_version="" key value
    info="$(remote_task app-info)"
    while IFS='=' read -r key value; do
        case "$key" in
            arch) server_arch="$value" ;;
            current_release) current="$value" ;;
            db_present) db_present="$value" ;;
            db_schema_version) db_version="$value" ;;
        esac
    done <<< "$info"

    local want_arch
    case "$RID" in
        linux-arm64) want_arch=aarch64 ;;
        linux-x64) want_arch=x86_64 ;;
    esac
    [[ "$server_arch" == "$want_arch" ]] || die "The server is $server_arch, but RID=$RID builds for $want_arch. Set RID in deploy.env."
    say "Server: $server_arch. Current release: ${current:-none}."

    local expected
    expected="$(expected_schema_version)"
    if [[ "$db_present" != yes ]]; then
        warn "The server has no database yet. The app will start but fail its health check until you run deploy-db.sh."
    elif [[ -n "$expected" && "$db_version" != "$expected" ]]; then
        warn "The server's database has schema version ${db_version:-unknown}, and this app expects version $expected.
The health check will fail until you deploy a database with schema version $expected (deploy-db.sh)."
    fi

    local id out
    id="$(release_id)"
    out="$WORK_DIR/publish/$RID"
    step "Building BeastieBot3.Site for $RID (release $id)"
    [[ -n "$WORK_DIR" && "$out" == "$WORK_DIR/publish/"* ]] || die "Unexpected publish folder: $out"
    rm -rf -- "$out"
    dotnet publish "$REPO_ROOT/BeastieBot3.Site/BeastieBot3.Site.csproj" \
        -c Release -r "$RID" --self-contained -o "$out" -nologo
    [[ -x "$out/BeastieBot3.Site" ]] || die "The build did not produce $out/BeastieBot3.Site."
    {
        printf 'release=%s\n' "$id"
        printf 'built_at_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf 'commit=%s\n' "$(git -C "$REPO_ROOT" rev-parse HEAD 2>/dev/null || echo unknown)"
        printf 'rid=%s\n' "$RID"
    } > "$out/release-info.txt"

    step "Uploading release $id"
    # -c compares file contents rather than times, so files that did not change are hard-linked
    # from the current release (--link-dest) instead of being uploaded again. Times are not kept
    # (no -t), because hard-linking needs every kept attribute to match.
    local link_dest=()
    if [[ -n "$current" ]]; then
        link_dest=(--link-dest="/srv/beastie/releases/$current/")
    fi
    rsync_to_server -rlpcz --delete --chmod=go+rX,go-w -h --info=stats1 ${link_dest[@]+"${link_dest[@]}"} \
        "$out/" "$TARGET:/srv/beastie/releases/$id/"

    step "Switching the site to release $id"
    local rc=0
    remote_task activate-app "$id" || rc=$?
    case "$rc" in
        0)
            check_public_health loopback-ok
            say ""
            say "Deployed release $id."
            say "To switch back to the previous release: deploy/oracle/deploy-app.sh --rollback"
            ;;
        3)
            say ""
            say "Deployed release $id. Next, deploy the database: deploy/oracle/deploy-db.sh"
            exit 3
            ;;
        *)
            say ""
            say "Release $id is live but failed its health check (details above)."
            say "To switch back to the previous release: deploy/oracle/deploy-app.sh --rollback"
            exit 2
            ;;
    esac
}

case "${1:-}" in
    "") deploy ;;
    --rollback) load_env; rollback ;;
    -h|--help) usage ;;
    *) die "Unknown option: $1 (use --rollback, or no options)" ;;
esac
