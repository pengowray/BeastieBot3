#!/usr/bin/env bash
# Shows the state of the site on the server: app release, service state, health check, live and
# previous databases, Caddy and free disk space. Changes nothing.
#
#   deploy/oracle/status.sh
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

case "${1:-}" in
    "") ;;
    -h|--help) sed -n '2,5p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "Unknown option: $1" ;;
esac

load_env
step "Server $HOST"
remote_task status
step "Public check"
check_public_health
