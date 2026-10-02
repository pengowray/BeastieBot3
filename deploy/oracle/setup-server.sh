#!/usr/bin/env bash
# Prepares an Ubuntu 24.04 VM to run the BeastieBot3 public site. Safe to run again: each step
# checks what is already in place, so a second run only updates what has changed (for example a
# new DOMAIN, Caddyfile.template or beastie-site.service).
#
# On your computer, run it with no arguments:
#   deploy/oracle/setup-server.sh
# It reads deploy.env, copies this script, server-tasks.sh, Caddyfile.template,
# caddy-admin-socket.conf and beastie-site.service to a temporary folder on the server, runs the
# script there with sudo, and deletes the folder.
#
# On the server it runs with --on-server (as root):
#   sudo bash setup-server.sh --on-server --domain species.example.org [--email you@example.org]
#        [--deploy-user ubuntu] [--auto-reboot-at 04:30]
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# --- On your computer ------------------------------------------------------------------------

run_from_workstation() {
    # shellcheck source=lib.sh
    source "$SCRIPT_DIR/lib.sh"
    load_env

    local files=(setup-server.sh server-tasks.sh Caddyfile.template caddy-admin-socket.conf beastie-site.service) f
    for f in "${files[@]}"; do
        [[ -f "$DEPLOY_DIR/$f" ]] || die "Missing file: $DEPLOY_DIR/$f"
    done

    local args=(--on-server --domain "$DOMAIN" --deploy-user "$SSH_USER")
    if [[ -n "$ACME_EMAIL" ]]; then
        args+=(--email "$ACME_EMAIL")
    fi
    if [[ -n "$AUTO_REBOOT_AT" ]]; then
        args+=(--auto-reboot-at "$AUTO_REBOOT_AT")
    fi

    step "Setting up $TARGET for https://$DOMAIN"
    # tar sends the files over the SSH connection's standard input; the setup script then runs
    # from the temporary folder.
    local remote_cmd
    remote_cmd="set -e; d=\$(mktemp -d); trap 'rm -rf \"\$d\"' EXIT; tar -xf - -C \"\$d\"; sudo bash \"\$d/setup-server.sh\" $(printf '%q ' "${args[@]}")"
    tar -C "$DEPLOY_DIR" -cf - "${files[@]}" | remote "bash -c $(printf '%q' "$remote_cmd")"

    step "Next steps (skip any you have already done)"
    say "1. Make sure the DNS A record for $DOMAIN points at $HOST."
    say "2. Deploy the database: deploy/oracle/deploy-db.sh"
    say "3. Deploy the app:      deploy/oracle/deploy-app.sh"
}

# --- On the server ---------------------------------------------------------------------------

DOMAIN=""
ACME_EMAIL=""
DEPLOY_USER="${SUDO_USER:-}"
AUTO_REBOOT_AT=""

BASE=/srv/beastie
SERVICE=beastie-site
SERVICE_USER=beastie
CADDY_DROPIN=/etc/systemd/system/caddy.service.d/beastie-admin-socket.conf
CADDY_ADMIN_SOCKET=/run/caddy/admin.sock

log()  { printf '\n== %s\n' "$*"; }
info() { printf '%s\n' "$*"; }
warn() { printf 'Warning: %s\n' "$*" >&2; }
fail() { printf 'Error: %s\n' "$*" >&2; exit 1; }

systemd_running() {
    [[ -d /run/systemd/system ]]
}

apt_get() {
    DEBIAN_FRONTEND=noninteractive NEEDRESTART_MODE=a NEEDRESTART_SUSPEND=1 \
        apt-get -y -q -o Dpkg::Options::=--force-confdef -o Dpkg::Options::=--force-confold "$@" </dev/null
}

# Name of the newest libicu package that apt knows about (libicu74 on Ubuntu 24.04).
libicu_package() {
    apt-cache pkgnames libicu 2>/dev/null | grep -E '^libicu[0-9]+$' | sort -V | tail -n 1
}

# The first of the given packages that apt knows about (package names differ between releases).
first_known_package() {
    local p
    for p in "$@"; do
        if apt-cache show "$p" >/dev/null 2>&1; then
            printf '%s' "$p"
            return 0
        fi
    done
}

check_system() {
    [[ $EUID -eq 0 ]] || fail "Run with sudo: sudo bash setup-server.sh --on-server ..."
    [[ -n "$DOMAIN" ]] || fail "--domain is required."
    [[ "$DOMAIN" =~ ^[A-Za-z0-9]([A-Za-z0-9.-]*[A-Za-z0-9])?$ ]] || fail "--domain must be a host name: $DOMAIN"
    [[ -z "$ACME_EMAIL" || "$ACME_EMAIL" =~ ^[^[:space:]\'\"@/|]+@[^[:space:]\'\"@/|]+$ ]] || fail "--email is not an email address: $ACME_EMAIL"
    [[ -z "$AUTO_REBOOT_AT" || "$AUTO_REBOOT_AT" =~ ^([01][0-9]|2[0-3]):[0-5][0-9]$ ]] || fail "--auto-reboot-at must be a time such as 04:30: $AUTO_REBOOT_AT"
    [[ -n "$DEPLOY_USER" ]] || fail "--deploy-user is required when the script is not run with sudo."
    [[ "$DEPLOY_USER" != root ]] || fail "--deploy-user must be the SSH user you deploy with, not root."
    id "$DEPLOY_USER" >/dev/null 2>&1 || fail "User $DEPLOY_USER does not exist on this server."
    for f in server-tasks.sh Caddyfile.template caddy-admin-socket.conf beastie-site.service; do
        [[ -f "$SCRIPT_DIR/$f" ]] || fail "Missing file next to this script: $SCRIPT_DIR/$f"
    done
    # Installing iptables-persistent removes ufw, and Oracle advises against ufw on its images.
    if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q '^Status: active'; then
        fail "ufw is active. This script opens ports with iptables, as Oracle's images expect. Turn ufw off first (sudo ufw disable), or open ports 80 and 443 in ufw and set up the rest by hand."
    fi

    # shellcheck source=/dev/null
    . /etc/os-release
    [[ "${ID:-}" == ubuntu ]] || fail "This script supports Ubuntu only. This server runs ${PRETTY_NAME:-an unknown system}."
    if [[ "${VERSION_ID:-}" != 24.04 ]]; then
        warn "Written for Ubuntu 24.04; this server runs $PRETTY_NAME. Continuing."
    fi
    info "System: $PRETTY_NAME, $(dpkg --print-architecture), $(awk '/MemTotal/ {printf "%.1f GB RAM", $2 / 1048576}' /proc/meminfo)"
}

install_packages() {
    log "Updating packages"
    # iptables-persistent asks whether to save the current rules when it is installed. Oracle's
    # images already have it; on other images, answer yes so the install does not stop to ask.
    echo 'iptables-persistent iptables-persistent/autosave_v4 boolean true' | debconf-set-selections
    echo 'iptables-persistent iptables-persistent/autosave_v6 boolean true' | debconf-set-selections
    apt_get update
    apt_get upgrade

    local icu ssl
    icu="$(libicu_package)"
    ssl="$(first_known_package libssl3t64 libssl3)"
    [[ -n "$icu" ]] || fail "Could not find a libicu package. The .NET app needs ICU."
    # curl, gnupg, keyrings: adding Caddy's apt repository. zstd, rsync, python3: deploy-db.sh and
    # server-tasks.sh. libicu, libssl, libstdc++6, zlib1g, libgcc-s1: the self-contained .NET app.
    apt_get -qq install ca-certificates curl gnupg debian-keyring debian-archive-keyring apt-transport-https \
        unattended-upgrades iptables iptables-persistent netfilter-persistent \
        zstd rsync python3 \
        "$icu" ${ssl:+"$ssl"} libstdc++6 zlib1g libgcc-s1
}

configure_unattended_upgrades() {
    log "Automatic security updates"
    cat > /etc/apt/apt.conf.d/20auto-upgrades <<'EOF'
// Written by BeastieBot3 setup-server.sh: update package lists and install security updates daily.
APT::Periodic::Update-Package-Lists "1";
APT::Periodic::Unattended-Upgrade "1";
EOF
    local reboot_conf=/etc/apt/apt.conf.d/52beastie-auto-reboot
    if [[ -n "$AUTO_REBOOT_AT" ]]; then
        cat > "$reboot_conf" <<EOF
// Written by BeastieBot3 setup-server.sh: restart at $AUTO_REBOOT_AT when an update needs a reboot.
Unattended-Upgrade::Automatic-Reboot "true";
Unattended-Upgrade::Automatic-Reboot-Time "$AUTO_REBOOT_AT";
EOF
        info "Security updates install daily. The server restarts at $AUTO_REBOOT_AT when an update needs a reboot."
    else
        rm -f "$reboot_conf"
        info "Security updates install daily. Restart the server yourself when an update needs a reboot."
    fi
}

configure_swap() {
    local mem_kb
    mem_kb="$(awk '/MemTotal/ {print $2}' /proc/meminfo)"
    if (( mem_kb >= 2 * 1024 * 1024 )); then
        return 0
    fi
    log "Swap file (this server has less than 2 GB of RAM)"
    if [[ -n "$(swapon --show --noheadings 2>/dev/null)" ]]; then
        info "Swap is already active."
        return 0
    fi
    if [[ ! -f /swapfile ]]; then
        fallocate -l 2G /swapfile
        chmod 600 /swapfile
        mkswap /swapfile >/dev/null
    fi
    if ! grep -qE '^/swapfile[[:space:]]' /etc/fstab; then
        echo '/swapfile none swap sw 0 0' >> /etc/fstab
    fi
    if swapon /swapfile; then
        info "Added a 2 GB swap file: /swapfile"
    else
        warn "Could not turn on /swapfile. It will be used after the next reboot."
    fi
}

create_user_and_folders() {
    log "Service user and folders"
    if ! id "$SERVICE_USER" >/dev/null 2>&1; then
        useradd --system --user-group --home-dir /nonexistent --no-create-home \
            --shell /usr/sbin/nologin "$SERVICE_USER"
        info "Created system user $SERVICE_USER (no login shell)."
    fi
    # The site reads app and data; only root and the deploy user can change them.
    install -d -m 755 -o root -g root "$BASE" "$BASE/data"
    install -d -m 755 -o "$DEPLOY_USER" -g "$DEPLOY_USER" "$BASE/releases" "$BASE/incoming"
    info "$BASE/data: root, read-only for the site."
    info "$BASE/releases and $BASE/incoming: writable by $DEPLOY_USER for uploads."
}

# Rule numbers below count the "-A INPUT" lines that "-S INPUT" prints, in order. They are the
# numbers that "-L INPUT --line-numbers" shows and that -I, -D and "-S INPUT <number>" take.

# Number and target of the first REJECT or DROP rule in the INPUT chain ("7 REJECT"), or nothing
# when there is none.
first_reject_rule() {
    "$1" -S INPUT | awk '$1 == "-A" { n++; if ($0 ~ / -j (REJECT|DROP)( |$)/) { print n, ($0 ~ / -j REJECT/ ? "REJECT" : "DROP"); exit } }'
}

# Number and text (separated by a tab) of the first INPUT rule that accepts new TCP connections to
# a port from anywhere, as "-S INPUT" prints it, or nothing. The text is matched loosely because
# iptables may print the state match in another form than the one it was added with.
accept_rule_for_port() {
    "$1" -S INPUT | awk -v port="$2" '
        $1 == "-A" {
            n++
            line = $0 " "
            if (line ~ (" --dport " port " ") && line ~ / -p tcp / && line ~ / -j ACCEPT $/ &&
                line ~ /NEW/ && line !~ / (-s|-d|-i|-o|!) /) {
                print n "\t" $0
                exit
            }
        }'
}

# Adds an ACCEPT rule for a TCP port to the INPUT chain of iptables or ip6tables when it is missing.
# Oracle's Ubuntu images end the INPUT chain with a REJECT rule, so the new rule goes in just
# before the first REJECT or DROP rule. An ACCEPT rule that is already there but comes after the
# first REJECT or DROP rule never matches (a common mistake when the rule is added by hand with -A),
# so it is moved: a copy goes in before the REJECT or DROP rule and the old one is deleted. No
# other rule is removed.
open_port() {
    local tool="$1" port="$2"
    local rule=(-p tcp -m state --state NEW -m tcp --dport "$port" -j ACCEPT)
    local reject reject_n="" target="" policy
    reject="$(first_reject_rule "$tool")"
    if [[ -n "$reject" ]]; then
        reject_n="${reject%% *}"
        target="${reject#* }"
    fi
    if "$tool" -C INPUT "${rule[@]}" 2>/dev/null; then
        local found accept_n accept_text
        found="$(accept_rule_for_port "$tool" "$port")"
        if [[ -z "$found" ]]; then
            info "$tool: port $port is already open."
            warn "Could not tell where the $tool rule for port $port is. It only works if it comes before the first REJECT rule; check with: sudo $tool -L INPUT --line-numbers -n"
            return 0
        fi
        accept_n="${found%%$'\t'*}"
        accept_text="${found#*$'\t'}"
        if [[ -z "$reject_n" ]] || (( accept_n < reject_n )); then
            info "$tool: port $port is already open."
            return 0
        fi
        "$tool" -I INPUT "$reject_n" "${rule[@]}"
        # The insert moved the old rule down by one. Delete it only if it is still the same rule.
        if [[ "$("$tool" -S INPUT $(( accept_n + 1 )))" == "$accept_text" ]]; then
            "$tool" -D INPUT $(( accept_n + 1 ))
            info "$tool: port $port had an ACCEPT rule (rule $accept_n) after the $target rule (rule $reject_n), so it had no effect. Moved it to rule $reject_n, before the $target rule."
        else
            info "$tool: port $port had an ACCEPT rule (rule $accept_n) after the $target rule (rule $reject_n), so it had no effect. Added a copy as rule $reject_n, before the $target rule. The old rule, now rule $(( accept_n + 1 )), is still there and has no effect."
        fi
        return 0
    fi
    policy="$("$tool" -S INPUT | awk '$1 == "-P" { print $3 }')"
    if [[ -n "$reject_n" ]]; then
        "$tool" -I INPUT "$reject_n" "${rule[@]}"
        info "$tool: opened port $port (rule $reject_n, before the first REJECT or DROP rule)."
    elif [[ "$policy" != ACCEPT ]]; then
        "$tool" -A INPUT "${rule[@]}"
        info "$tool: opened port $port (appended; the INPUT policy is $policy)."
    else
        info "$tool: port $port is open (the INPUT chain accepts everything)."
    fi
}

configure_firewall() {
    log "Firewall (iptables)"
    if ! iptables -S INPUT >/dev/null 2>&1; then
        warn "iptables does not work here (inside a container?). Skipping the firewall rules."
        return 0
    fi
    local port
    for port in 80 443; do
        open_port iptables "$port"
    done
    if ip6tables -S INPUT >/dev/null 2>&1; then
        for port in 80 443; do
            open_port ip6tables "$port"
        done
    fi
    local output
    if ! output="$(netfilter-persistent save 2>&1)"; then
        printf '%s\n' "$output" >&2
        fail "netfilter-persistent save failed. The new rules are active but will be lost at the next reboot."
    fi
    info "Saved the rules to /etc/iptables/rules.v4 and rules.v6 (netfilter-persistent)."
}

install_caddy() {
    log "Caddy"
    local keyring=/usr/share/keyrings/caddy-stable-archive-keyring.gpg
    local list=/etc/apt/sources.list.d/caddy-stable.list
    if [[ ! -s "$keyring" || ! -s "$list" ]]; then
        # Caddy's official apt repository (caddyserver.com/docs/install), which has arm64 packages.
        curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --batch --yes --dearmor -o "$keyring"
        curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' > "$list"
        chmod o+r "$keyring" "$list"
        apt_get update
    fi
    if ! dpkg -s caddy >/dev/null 2>&1; then
        apt_get install caddy
    fi
    info "$(caddy version | head -n 1)"
}

render_caddyfile() {
    local out="$1"
    if [[ -n "$ACME_EMAIL" ]]; then
        sed -e "s|__DOMAIN__|$DOMAIN|g" -e "s|__ACME_EMAIL__|$ACME_EMAIL|g" "$SCRIPT_DIR/Caddyfile.template" > "$out"
    else
        sed -e "s|__DOMAIN__|$DOMAIN|g" -e '/__ACME_EMAIL__/d' "$SCRIPT_DIR/Caddyfile.template" > "$out"
    fi
}

configure_caddy() {
    log "Caddy settings for $DOMAIN"
    # The drop-in goes in first: the new Caddyfile puts the admin API in /run/caddy, which systemd
    # creates only when the drop-in is in place.
    local dropin_changed=no caddyfile_changed=no
    if [[ -f "$CADDY_DROPIN" ]] && cmp -s "$SCRIPT_DIR/caddy-admin-socket.conf" "$CADDY_DROPIN"; then
        info "$CADDY_DROPIN is up to date."
    else
        install -d -m 755 -o root -g root "$(dirname "$CADDY_DROPIN")"
        install -m 644 -o root -g root "$SCRIPT_DIR/caddy-admin-socket.conf" "$CADDY_DROPIN"
        dropin_changed=yes
        info "Installed $CADDY_DROPIN."
        if systemd_running; then
            systemctl daemon-reload
        fi
    fi

    local target=/etc/caddy/Caddyfile tmp
    tmp="$(mktemp)"
    render_caddyfile "$tmp"
    chmod 644 "$tmp"
    if [[ -f "$target" ]] && cmp -s "$tmp" "$target"; then
        rm -f "$tmp"
        info "$target is up to date."
    else
        # Validate as the caddy user: run as root, caddy validate would create the access log file
        # owned by root, and the caddy service could not write to it.
        if ! runuser -u caddy -- caddy validate --config "$tmp" --adapter caddyfile >/tmp/caddy-validate.log 2>&1; then
            cat /tmp/caddy-validate.log >&2
            rm -f "$tmp"
            fail "The new Caddyfile is not valid. $target was not changed."
        fi
        if [[ -f "$target" ]]; then
            cp -p "$target" "$target.previous"
            info "Saved the old file as $target.previous."
        fi
        install -m 644 -o root -g root "$tmp" "$target"
        rm -f "$tmp"
        caddyfile_changed=yes
        info "Wrote $target."
    fi

    systemd_running || return 0
    systemctl enable caddy >/dev/null 2>&1 || true
    # A reload sends the new configuration to the running Caddy through the admin socket named in
    # the new Caddyfile. When Caddy does not answer there (the first run with the socket setting,
    # when Caddy still listens on localhost:2019, or Caddy is not running) or the drop-in changed,
    # Caddy has to be restarted instead. A restart keeps the certificates, which are stored on disk.
    if [[ "$dropin_changed" == yes ]] || ! caddy_admin_answers; then
        systemctl restart caddy
        info "Restarted Caddy. It requests the certificate for $DOMAIN once DNS points at this server."
    elif [[ "$caddyfile_changed" == yes ]]; then
        systemctl reload caddy
        info "Reloaded Caddy. It requests the certificate for $DOMAIN once DNS points at this server."
    fi
    if ! caddy_admin_answers; then
        warn "Caddy's admin API does not answer on $CADDY_ADMIN_SOCKET, so systemctl reload caddy will fail. Check: sudo journalctl -u caddy -n 50"
    fi
}

# Whether the running Caddy answers on its admin socket. A socket file alone proves nothing: Caddy
# can leave one behind when it stops.
caddy_admin_answers() {
    curl -fsS --max-time 5 --unix-socket "$CADDY_ADMIN_SOCKET" -o /dev/null http://localhost/config/ 2>/dev/null
}

install_service() {
    log "Service $SERVICE"
    install -m 755 -o root -g root "$SCRIPT_DIR/server-tasks.sh" /usr/local/sbin/beastie-site
    info "Installed /usr/local/sbin/beastie-site (run: sudo beastie-site status)."

    local unit=/etc/systemd/system/$SERVICE.service changed=no
    if ! [[ -f "$unit" ]] || ! cmp -s "$SCRIPT_DIR/beastie-site.service" "$unit"; then
        install -m 644 -o root -g root "$SCRIPT_DIR/beastie-site.service" "$unit"
        changed=yes
        info "Installed $unit."
    else
        info "$unit is up to date."
    fi
    if ! systemd_running; then
        warn "systemd is not running here. Skipping systemctl."
        return 0
    fi
    systemctl daemon-reload
    systemctl enable "$SERVICE" >/dev/null 2>&1
    info "$SERVICE starts at boot."
    if [[ -x "$BASE/app/BeastieBot3.Site" ]]; then
        if [[ "$changed" == yes ]]; then
            systemctl restart "$SERVICE"
            info "Restarted $SERVICE with the new unit file."
        fi
    else
        info "No app deployed yet. deploy-app.sh starts the service."
    fi
}

run_on_server() {
    while (( $# > 0 )); do
        case "$1" in
            --on-server) shift ;;
            --domain) DOMAIN="${2:-}"; shift 2 ;;
            --email) ACME_EMAIL="${2:-}"; shift 2 ;;
            --deploy-user) DEPLOY_USER="${2:-}"; shift 2 ;;
            --auto-reboot-at) AUTO_REBOOT_AT="${2:-}"; shift 2 ;;
            *) fail "Unknown option: $1" ;;
        esac
    done
    check_system
    install_packages
    configure_unattended_upgrades
    configure_swap
    create_user_and_folders
    configure_firewall
    install_caddy
    configure_caddy
    install_service

    log "Done"
    if [[ -f /var/run/reboot-required ]]; then
        info "Updates installed by this run need a reboot: sudo reboot"
    fi
}

case "${1:-}" in
    --on-server) run_on_server "$@" ;;
    "") run_from_workstation ;;
    -h|--help) awk 'NR > 1 && /^#/ { sub(/^# ?/, ""); print; next } NR > 1 { exit }' "$0" ;;
    *) printf 'Unknown option: %s (use --on-server on the server, or no options on your computer)\n' "$1" >&2; exit 1 ;;
esac
