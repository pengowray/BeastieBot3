# shellcheck shell=bash
# Helpers shared by the deploy scripts in this folder, which source this file. They run on your
# own computer and reach the server over SSH.

DEPLOY_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$DEPLOY_DIR/../.." && pwd)"

say()  { printf '%s\n' "$*"; }
step() { printf '\n== %s\n' "$*"; }
warn() { printf 'Warning: %s\n' "$*" >&2; }
die()  { printf 'Error: %s\n' "$*" >&2; exit 1; }

# Reads deploy.env (or the file named by DEPLOY_ENV), applies defaults and checks the values the
# scripts put into commands and configuration files.
load_env() {
    local env_file="${DEPLOY_ENV:-$DEPLOY_DIR/deploy.env}"
    [[ -f "$env_file" ]] || die "Settings file not found: $env_file
Copy deploy/oracle/deploy.env.example to deploy/oracle/deploy.env and fill it in."
    # shellcheck source=/dev/null
    source "$env_file"

    HOST="${HOST:-}"
    DOMAIN="${DOMAIN:-}"
    SSH_USER="${SSH_USER:-ubuntu}"
    SSH_PORT="${SSH_PORT:-22}"
    SSH_KEY="${SSH_KEY:-}"
    RID="${RID:-linux-arm64}"
    ACME_EMAIL="${ACME_EMAIL:-}"
    AUTO_REBOOT_AT="${AUTO_REBOOT_AT:-}"
    HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-60}"
    ZSTD_LEVEL="${ZSTD_LEVEL:-10}"
    WORK_DIR="${WORK_DIR:-${XDG_CACHE_HOME:-$HOME/.cache}/beastiebot-deploy}"

    [[ -n "$HOST" ]] || die "HOST is not set in $env_file."
    [[ -n "$DOMAIN" ]] || die "DOMAIN is not set in $env_file."
    [[ "$HOST" =~ ^[A-Za-z0-9.:-]+$ ]] || die "HOST must be an IP address or host name: $HOST"
    [[ "$DOMAIN" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] || die "DOMAIN must be a host name such as species.example.org: $DOMAIN"
    [[ "$SSH_USER" =~ ^[a-z_][a-z0-9_-]*$ ]] || die "SSH_USER is not a valid user name: $SSH_USER"
    [[ "$SSH_PORT" =~ ^[0-9]+$ ]] || die "SSH_PORT must be a number: $SSH_PORT"
    [[ -z "$ACME_EMAIL" || "$ACME_EMAIL" =~ ^[^[:space:]\'\"@]+@[^[:space:]\'\"@]+$ ]] || die "ACME_EMAIL is not an email address: $ACME_EMAIL"
    [[ -z "$AUTO_REBOOT_AT" || "$AUTO_REBOOT_AT" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || die "AUTO_REBOOT_AT must be a time such as 04:30: $AUTO_REBOOT_AT"
    [[ "$HEALTH_TIMEOUT" =~ ^[0-9]+$ ]] || die "HEALTH_TIMEOUT must be a number of seconds: $HEALTH_TIMEOUT"
    case "$RID" in
        linux-arm64|linux-x64) ;;
        *) die "RID must be linux-arm64 or linux-x64: $RID" ;;
    esac

    mkdir -p "$WORK_DIR"
    TARGET="$SSH_USER@$HOST"
    # One SSH connection is opened and reused by every ssh and rsync call in a run, so a key
    # passphrase is asked for once. The socket path must stay under about 100 characters.
    local socket_dir="${XDG_RUNTIME_DIR:-$WORK_DIR}"
    SSH_OPTS=(-p "$SSH_PORT" -o ConnectTimeout=15 -o ServerAliveInterval=30
        -o ControlMaster=auto -o "ControlPath=$socket_dir/beastie-ssh-%r@%h:%p" -o ControlPersist=60)
    if [[ -n "$SSH_KEY" ]]; then
        SSH_OPTS+=(-i "$SSH_KEY")
    fi
    trap close_ssh EXIT
}

close_ssh() {
    ssh "${SSH_OPTS[@]}" -O exit "$TARGET" >/dev/null 2>&1 || true
}

# Runs a shell command on the server as SSH_USER.
remote() {
    # shellcheck disable=SC2029  # the command is meant to be built here and run there
    ssh "${SSH_OPTS[@]}" "$TARGET" "$@"
}

# Runs one task from server-tasks.sh on the server as root. The local copy of server-tasks.sh is
# sent each time, so the server runs the version that matches these scripts.
remote_task() {
    local args
    args="$(printf '%q ' "$@")"
    # shellcheck disable=SC2029  # the arguments are quoted with printf %q above
    ssh "${SSH_OPTS[@]}" "$TARGET" "sudo env HEALTH_TIMEOUT=$HEALTH_TIMEOUT bash -s -- $args" < "$DEPLOY_DIR/server-tasks.sh"
}

# The ssh command for rsync -e. rsync splits that string on spaces and understands single quotes
# but not backslashes, so each argument is put in single quotes.
rsync_ssh_command() {
    local cmd="ssh" arg
    for arg in "${SSH_OPTS[@]}"; do
        [[ "$arg" != *"'"* ]] || die "SSH settings must not contain a single quote: $arg"
        cmd+=" '$arg'"
    done
    printf '%s' "$cmd"
}

# Copies files to the server with rsync over the shared SSH connection.
rsync_to_server() {
    rsync -e "$(rsync_ssh_command)" "$@"
}

# Puts ~/.dotnet on PATH when dotnet is not already there.
find_dotnet() {
    if ! command -v dotnet >/dev/null 2>&1 && [[ -x "$HOME/.dotnet/dotnet" ]]; then
        PATH="$HOME/.dotnet:$PATH"
    fi
    command -v dotnet >/dev/null 2>&1 || die "dotnet not found. Install the .NET 10 SDK, or put it in ~/.dotnet."
}

# Schema version that the site built from this checkout expects (SiteDbSchema.Version), or empty.
expected_schema_version() {
    local file="$REPO_ROOT/BeastieBot3.Shared/SiteData/SiteDbSchema.cs"
    [[ -f "$file" ]] || return 0
    sed -n 's/^[[:space:]]*public const int Version = \([0-9][0-9]*\);.*/\1/p' "$file" | head -n 1
}

# Checks https://DOMAIN/healthz from this computer. Only reports: DNS changes and the first
# certificate can take a while. Pass "loopback-ok" when the check on the server has just passed,
# which narrows down where the problem is; status.sh passes "status-only".
check_public_health() {
    local url="https://$DOMAIN/healthz" error
    if error="$(curl -fsS --max-time 15 -o /dev/null "$url" 2>&1)"; then
        say "Public check passed: $url"
        return 0
    fi
    warn "Public check failed: $url
  ${error}"
    if [[ "${1:-}" == loopback-ok ]]; then
        say "The site answers on the server itself, so check the path from the internet to Caddy:
  - the DNS A record for $DOMAIN points at the VM's public IP ($HOST if HOST is that IP),
  - the VCN security list allows TCP 80 and 443 from 0.0.0.0/0,
  - the VM's own firewall lets them in: ssh to the server and run  sudo iptables -L INPUT --line-numbers -n
    (the ACCEPT rules for ports 80 and 443 must come before the REJECT rule; setup-server.sh moves them there),
  - Caddy has its certificate: ssh to the server and run  sudo journalctl -u caddy -n 50"
    fi
}
