#!/usr/bin/env bash
# Prints a usage report of the site from Caddy's access logs on the server: page views, visitors,
# searches, status update runs and crawler requests per day, then the most viewed taxa, the most
# frequent searches and the referring sites. Counts only: the report shows no IP addresses. Changes
# nothing. The logs go back 14 days.
#
#   deploy/oracle/usage.sh [--days N] [--top N]
set -euo pipefail
# shellcheck source=lib.sh
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

args=()
while [[ $# -gt 0 ]]; do
    case "$1" in
        --days|--top)
            [[ $# -ge 2 && "$2" =~ ^[0-9]+$ ]] || die "$1 needs a number"
            args+=("$1" "$2"); shift 2 ;;
        -h|--help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "${BASH_SOURCE[0]}"; exit 0 ;;
        *) die "Unknown option: $1" ;;
    esac
done

load_env
step "Usage of https://$DOMAIN"
# The logs are readable by the caddy user only, so the report runs as root.
remote "sudo python3 - ${args[*]:-}" < "$DEPLOY_DIR/usage-report.py"
