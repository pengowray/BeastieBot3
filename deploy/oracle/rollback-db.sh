#!/usr/bin/env bash
# Swaps the live database (/srv/beastie/data/site.sqlite) with the previous one
# (site.sqlite.prev) and restarts the site. Running it a second time puts the newer database back.
#
#   deploy/oracle/rollback-db.sh
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

case "${1:-}" in
    "") ;;
    -h|--help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "${BASH_SOURCE[0]}"; exit 0 ;;
    *) die "Unknown option: $1" ;;
esac

load_env
step "Switching $DOMAIN back to the previous database"
rc=0
remote_task rollback-db || rc=$?
case "$rc" in
    0) check_public_health loopback-ok ;;
    3) ;;
    *) exit "$rc" ;;
esac
