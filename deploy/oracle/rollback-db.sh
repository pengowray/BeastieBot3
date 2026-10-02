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
    -h|--help) sed -n '2,5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "Unknown option: $1" ;;
esac

load_env
step "Switching $DOMAIN back to the previous database"
remote_task rollback-db
check_public_health
