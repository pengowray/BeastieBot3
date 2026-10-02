# Deploying the public site to Oracle Cloud

These scripts put BeastieBot3.Site and its database on an Oracle Cloud Always Free VM, behind Caddy
for HTTPS.

**Testing status.** The scripts have not been run on a real Oracle VM. They pass `bash -n` and
ShellCheck 0.10.0, and the whole cycle ran in an Ubuntu 24.04 container (podman, systemd as PID 1,
x86_64, with the INPUT rules from Oracle's tutorial): server setup (run three times), database deploys,
automatic database rollback, manual rollbacks, app deploys with the real BeastieBot3.Site build
running under the hardened systemd unit, release pruning, and HTTPS through Caddy. See
[What was not tested](#what-was-not-tested) before the first real deploy.

## What you get

- One Ampere A1 VM running Ubuntu 24.04 (arm64).
- Caddy on ports 80 and 443. It gets and renews a Let's Encrypt certificate for your domain,
  redirects http to https, compresses responses, adds security headers and writes an access log.
- The site as a self-contained .NET app (no .NET install on the server), run by systemd as the
  user `beastie` on `127.0.0.1:5080`, with systemd's sandboxing options turned on. The app can read
  its own files and the database, and can write only to `/var/lib/beastie-site` and a private
  `/tmp`.
- The site database at `/srv/beastie/data/site.sqlite`, opened read-only.
- Daily security updates (unattended-upgrades).
- Scripts you run on your own computer:

  | Script | What it does |
  |---|---|
  | `setup-server.sh` | Prepares the VM. Run once, and again after changing `deploy.env`, `Caddyfile.template` or `beastie-site.service`. |
  | `deploy-app.sh` | Builds the site, uploads it as a new release, switches to it and checks `/healthz`. `--rollback` switches back to the release built before the current one. |
  | `deploy-db.sh` | Uploads a new `site.sqlite`, switches to it and checks `/healthz`. Puts the old database back when the check fails. |
  | `rollback-db.sh` | Swaps the live database with the previous one. |
  | `status.sh` | Shows the release, service state, health check, databases and free disk space. |

  `lib.sh` and `server-tasks.sh` hold code the other scripts share. `server-tasks.sh` runs on the
  server; a copy is installed there as `/usr/local/sbin/beastie-site`.

### What you need on your computer

- Linux or macOS with bash 4.4 or later (on Windows, use WSL), `ssh`, `rsync`, `curl`, `python3`
  and `git`. `zstd` is optional; without it the database upload uses gzip and is larger.
- The .NET 10 SDK. The scripts also look for it in `~/.dotnet`.
- An SSH key pair.

## In the Oracle Cloud console

Oracle changed the Always Free allowance in 2026. The facts below are from Oracle's documentation
and FAQ as fetched on 2026-10-02 and 2026-10-03.

1. **Account.** Sign up at oracle.com/cloud/free. You need a credit card, or a debit card that works
   like one (Oracle does not accept prepaid, virtual, single-use or PIN debit cards). Oracle places
   a temporary hold on the card. One free account per person.

2. **Home region.** You choose it at sign-up and cannot change it later. Always Free VMs and
   volumes can only be created in the home region. Pick one near your readers, and avoid South
   Korea North (Chuncheon), which has no Ampere A1. Some regions run out of A1 capacity more often
   than others; look for recent reports before you choose.

3. **Upgrade to Pay As You Go (PAYG), and set a budget alert.**
   - Why: Oracle may stop an Always Free VM that is idle. A VM is idle when, over 7 days, its
     95th-percentile CPU use is under 20%, network use is under 20%, and (on A1) memory use is under
     20%. A read-only site with a database of a few hundred MB on a 12 GB VM is very likely to
     meet all three.
   - Oracle's emails to customers in 2023 said that PAYG accounts are not stopped for being idle,
     and are not charged while all usage stays inside the Always Free limits. The current
     documentation does not mention this exemption, so treat it as likely rather than certain.
   - Oracle's FAQ also says PAYG accounts have different capacity limits, and users report that
     A1 VMs are easier to get after upgrading.
   - Upgrade under Billing (the Upgrade page). Upgrade before the 30-day trial ends.
   - Then create a budget under Billing and Cost Management, Budgets, with an email alert at
     US$1 of actual spending. Anything that is not Always Free is billed, and the alert tells you
     when that happens.

4. **Network.** Networking, Virtual Cloud Networks, Start VCN Wizard, "Create VCN with Internet
   Connectivity". Keep the defaults. This makes a public subnet and an internet gateway.

5. **The VM.** Compute, Instances, Create instance:
   - **Shape:** VM.Standard.A1.Flex with **2 OCPU and 12 GB memory at most**. That is the whole
     Always Free A1 allowance since 2026-06-15 (it was 4 OCPU and 24 GB). If a tenancy has more A1
     than the allowance, Oracle disables all its A1 VMs and deletes them after 30 days unless the
     account is upgraded. The allowance counts every A1 VM in the tenancy together.
   - **Image:** Canonical Ubuntu 24.04, the aarch64 build. The scripts support Ubuntu only.
   - **Boot volume:** set a custom size. Always Free gives 200 GB of block storage in total, and a
     larger volume is also faster:

     | Boot volume | IOPS | Throughput |
     |---|---|---|
     | 50 GB (default) | 3,000 | 24 MB/s |
     | 100 GB | 6,000 | 48 MB/s |
     | 150 GB | 9,000 | 72 MB/s |
     | 200 GB | 12,000 | 96 MB/s |

     Use 200 GB if this is your only VM, or 150 GB to leave room for a second VM (each needs at
     least 47 GB).
   - **Networking:** the VCN and public subnet from step 4, with "Assign a public IPv4 address"
     turned on. The address stays the same when the VM is stopped and started, and is lost when the
     VM is terminated. To keep an address that outlives the VM, reserve one under Networking, IP
     Management (check the "Reserved Public IP Count" limit first).
   - **SSH key:** paste your public key. The login user is `ubuntu`.

   If you get "Out of host capacity": try another availability domain in the same form, or try
   again later (Oracle says it can take several days). Upgrading to PAYG reportedly helps. Some
   people use scripts that retry the launch with the OCI CLI, such as hitrov/oci-arm-host-capacity.

6. **Open ports 80 and 443 in the VCN.** Networking, Virtual Cloud Networks, your VCN, the public
   subnet, Default Security List, Add Ingress Rules. Add two rules: source CIDR `0.0.0.0/0`, IP
   protocol TCP, destination port `80`; and the same for port `443`. Leave "Stateless" unticked.
   The VM also has its own firewall, which `setup-server.sh` opens.

Do not turn on ufw on the VM. Oracle's Ubuntu images have iptables rules that protect the boot
volume connection, and Oracle's known-issues page says ufw can stop the VM from booting.

## DNS

Caddy needs a host name whose DNS A record points at the VM's public IP before it can get a
certificate. Two choices:

- **Your own domain.** Add an A record, for example `species.example.org`, with the VM's public IP.
  No AAAA record: the wizard's VCN is IPv4 only. A domain costs about US$10 a year.
- **DuckDNS** (free). Create a subdomain at duckdns.org, such as `beastie.duckdns.org`, and set its
  IP to the VM's public IP once. No update client is needed, because the IP only changes if the VM
  is terminated. duckdns.org is on the Public Suffix List, so Let's Encrypt rate limits apply to
  your subdomain alone. DuckDNS has occasional outages.

Put the host name in `DOMAIN` in `deploy.env`.

## First-time server setup

1. Copy the settings file and fill it in:

   ```bash
   cp deploy/oracle/deploy.env.example deploy/oracle/deploy.env
   ```

   `HOST` is the VM's public IP, `DOMAIN` the host name from the DNS step, `RID` stays
   `linux-arm64` for an A1 VM. `deploy.env` is in `.gitignore`.

2. Connect once by hand and accept the server's host key:

   ```bash
   ssh ubuntu@<HOST>
   ```

   To check the fingerprint first, compare it with the SSH host key fingerprints that cloud-init
   writes to the VM's serial console, which you can capture in the console under your instance's
   Console history.

3. Run the setup from the repository root:

   ```bash
   deploy/oracle/setup-server.sh
   ```

   It copies its files to the VM and runs there with sudo, which needs passwordless sudo for the
   SSH user (the `ubuntu` user on Oracle's images has it). It takes a few minutes, mostly package
   updates. [What the scripts change on the server](#what-the-scripts-change-on-the-server) lists
   everything it does. If it ends with "need a reboot", run `ssh ubuntu@<HOST> sudo reboot`.

4. Deploy the database, then the app (next two sections). The app's health check needs a
   database, so the database goes first.

## Deploying the app

```bash
deploy/oracle/deploy-app.sh
```

1. Checks the server: that its CPU matches `RID`, and which schema version its database has. If
   the database's schema version differs from `SiteDbSchema.Version` in this checkout, it warns
   that the health check will fail until you deploy a matching database.
2. Builds the site: `dotnet publish -c Release -r <RID> --self-contained` into
   `~/.cache/beastiebot-deploy/publish/<RID>` (set `WORK_DIR` to change the folder; keep it out of
   Dropbox).
3. Uploads it to `/srv/beastie/releases/.upload-<UTC time>-<git commit>`, and renames that folder
   to `/srv/beastie/releases/<UTC time>-<git commit>` once the upload has finished and the folder
   has the app's executable. An interrupted upload therefore never counts as a release; the next
   deploy deletes it once it has been left for an hour. Files that are the same as in the current
   release are hard-linked on the server instead of uploaded, so after the first deploy an upload is
   usually a few MB. A commit name ending in `-dirty` means the checkout had uncommitted changes.
4. Points `/srv/beastie/app` at the new release (one rename) and deletes old releases. It keeps
   the newest 3, and always keeps the release that was live before this deploy, so that release is
   still there to go back to if the new one fails.
5. Restarts the service and checks `http://127.0.0.1:5080/healthz` on the server for up to 60
   seconds (`HEALTH_TIMEOUT`).
6. Checks `https://<DOMAIN>/healthz` from your computer. A failure here only gives a warning,
   because DNS and the first certificate can take a while.

If the health check fails, the script prints the service log and exits with an error, and the new
release stays live. It does not switch back by itself, because when the database schema changes
the new app and the new database have to go out one after the other (see
[Updating](#updating)). To switch back:

```bash
deploy/oracle/deploy-app.sh --rollback
```

This switches to the newest release whose name (its UTC build time) is older than the current
one, skipping any folder without the app's executable. It goes back by release name, not to the
release that was live before: if you have already rolled back once and then deployed again, the
first rollback can land on a release that failed before. Running it again goes back one more
release. `status.sh` lists the releases on disk.

## Deploying a new database after each IUCN release

After the local databases have been updated for a new Red List release, build the site database
and deploy it:

```bash
dotnet run --project BeastieBot3/BeastieBot3.csproj -- site build-db
deploy/oracle/deploy-db.sh
```

`deploy-db.sh` uses the path you give it, or `SITE_DB` from `deploy.env`, or `[Datastore]
site_sqlite` from `BeastieBot3/paths.ini`. Relative paths in `paths.ini` are resolved by the CLI
against its bin folder, so if yours is relative, pass the path instead.

What it does:

1. Checks the file: a SQLite database with a `meta` table and a `schema_version`, and `PRAGMA
   quick_check`. It prints the IUCN release, build time, schema version and taxon count, and warns
   if the schema version differs from this checkout's `SiteDbSchema.Version`.
2. If the database is in WAL mode, or has data waiting in its `-wal` file, it makes a copy in
   rollback journal mode with `VACUUM INTO`. The site opens the database read-only from a folder it
   cannot write to, which fails for a WAL-mode database, and copying a WAL database's main file on
   its own can lose most of its data.
3. Asks the server for the SHA-256 of the live database. If it is the same file, it stops.
4. Compresses the file with zstd (gzip if zstd is missing) into `WORK_DIR`.
5. Uploads it to `/srv/beastie/incoming` with `rsync --partial`. If the upload is interrupted, run
   the script again: it reuses the compressed file and continues the upload.
6. On the server: checks free disk space, decompresses, compares the SHA-256 with the local file,
   checks the SQLite header and `meta` table, and moves the file into place as `site.sqlite` in one
   rename. Then it restarts the service and checks `/healthz` on the server.
7. If the check passes, the database it replaced becomes `site.sqlite.prev`. If the check fails,
   the replaced database goes back into place, the new one is kept as `site.sqlite.failed`, and
   `site.sqlite.prev` is unchanged.

Do not run `deploy-db.sh` while `site build-db` is still writing the file. The server needs free
space for about 1.1 times the database size, plus 100 MB.

## Updating

| What | How |
|---|---|
| Ubuntu security updates | Installed daily. When one needs a reboot, `status.sh` shows "Reboot required: yes"; run `ssh ubuntu@<HOST> sudo reboot`. To let the server restart itself, set `AUTO_REBOOT_AT=04:30` (server time) in `deploy.env` and run `setup-server.sh` again. |
| Caddy | Comes from Caddy's own apt repository, which the daily updates do not cover. Run `setup-server.sh` again (it runs `apt-get upgrade`), or `ssh ubuntu@<HOST> sudo apt-get upgrade`. |
| .NET runtime | Part of the self-contained app. Security fixes reach the server only when you update the .NET 10 SDK on your computer and run `deploy-app.sh`. Microsoft releases .NET patches monthly. |
| The site | `deploy-app.sh`. |
| The domain, Caddy settings or the systemd unit | Edit `deploy.env`, `Caddyfile.template` or `beastie-site.service`, then run `setup-server.sh` again. It changes only what differs and restarts what it changed. |

**When the database schema version changes** (`SiteDbSchema.Version`), the site refuses a database
with a different version, so the app and the database have to change together:

1. Build the new database (`site build-db`).
2. Run `deploy-app.sh`. Its health check fails, because the server still has the old database.
   The site returns errors from now until step 3 finishes.
3. Run `deploy-db.sh` straight away. Its health check passes with the new app.

Doing it the other way round does not work: `deploy-db.sh` would see the old app reject the new
database and put the old database back.

## Checking health and logs

From your computer:

```bash
deploy/oracle/status.sh
curl https://<DOMAIN>/healthz        # "ok" when the site can open the database and its schema version matches
```

On the server (`ssh ubuntu@<HOST>`):

| Command | Shows |
|---|---|
| `sudo beastie-site status` | The same as `status.sh` |
| `systemctl status beastie-site` | Whether the site is running |
| `journalctl -u beastie-site -n 100` | The site's log (add `-f` to follow it) |
| `sudo journalctl -u caddy -n 50` | Caddy's log, including certificate requests and errors |
| `sudo tail -f /var/log/caddy/access.log` | Requests, one JSON object per line. Caddy starts a new file at 50 MB and deletes files after 14 days. |

To be told when the site goes down, point an external uptime checker at
`https://<DOMAIN>/healthz`.

## Rolling back to the previous database

```bash
deploy/oracle/rollback-db.sh
```

This swaps `site.sqlite` and `site.sqlite.prev`, restarts the site and checks `/healthz`. Running it
a second time puts the newer database back. Only one previous database is kept. On the server,
`sudo beastie-site rollback-db` does the same.

To roll back the app instead, see [Deploying the app](#deploying-the-app).

## What the scripts change on the server

`setup-server.sh`:

- Packages: runs `apt-get update` and `apt-get upgrade`, and installs `ca-certificates curl gnupg
  debian-keyring debian-archive-keyring apt-transport-https unattended-upgrades iptables
  iptables-persistent netfilter-persistent zstd rsync python3`, the libraries the .NET app needs
  (`libicu74`, `libssl3t64`, `libstdc++6`, `zlib1g`, `libgcc-s1`; the libicu and libssl names are
  looked up on the server), and `caddy`.
- `/etc/apt/apt.conf.d/20auto-upgrades`: overwritten, to turn on the daily package list update
  and unattended upgrade.
- `/etc/apt/apt.conf.d/52beastie-auto-reboot`: written only when `AUTO_REBOOT_AT` is set, and
  removed when it is not.
- `/swapfile` (2 GB) and a line in `/etc/fstab`: only on a VM with less than 2 GB of RAM, such as
  the x86 E2.1.Micro.
- User and group `beastie`: a system user with no login shell and no home folder.
- `/srv/beastie` and `/srv/beastie/data`, owned by root. `/srv/beastie/releases` and
  `/srv/beastie/incoming`, owned by the SSH user so that uploads need no sudo.
- iptables: an ACCEPT rule for new TCP connections to port 80 and to port 443 in the INPUT chain,
  inserted before the first REJECT or DROP rule, and only if missing. The same for ip6tables when
  its INPUT chain rejects or drops anything. Then `netfilter-persistent save` writes all current
  rules to `/etc/iptables/rules.v4` and `rules.v6`. No rule is removed and nothing is flushed. If
  ufw is active, the script stops before changing anything.
- Caddy's apt repository: `/usr/share/keyrings/caddy-stable-archive-keyring.gpg` and
  `/etc/apt/sources.list.d/caddy-stable.list`.
- `/etc/caddy/Caddyfile`, written from `Caddyfile.template` when it differs. The old file is kept
  as `/etc/caddy/Caddyfile.previous`. Caddy writes `/var/log/caddy/access.log` and keeps its
  certificates under `/var/lib/caddy`.
- `/etc/systemd/system/beastie-site.service`, enabled to start at boot. systemd creates
  `/var/lib/beastie-site` for the service.
- `/usr/local/sbin/beastie-site`, a copy of `server-tasks.sh`.

The deploy scripts:

- `deploy-app.sh` uploads into a folder `/srv/beastie/releases/.upload-<release>`, renames it to
  `/srv/beastie/releases/<release>` when the upload has finished, and points the symbolic link
  `/srv/beastie/app` at it. It deletes all releases but the newest 3 and the one that was live
  before the switch, and deletes `.upload-` folders that have been left for over an hour.
- `deploy-db.sh` uploads to `/srv/beastie/incoming` (the file is deleted after it is installed) and
  writes `/srv/beastie/data/site.sqlite`, `site.sqlite.prev` and, after a failed deploy,
  `site.sqlite.failed`. While it works it also uses `site.sqlite.new` and `site.sqlite.outgoing`.
  `rollback-db.sh` uses `site.sqlite.swap` for a moment.
- All three restart the `beastie-site` service.

Nothing changes SSH settings, users other than `beastie`, or ufw.

On your computer, the scripts use `~/.cache/beastiebot-deploy` (`WORK_DIR`) for the published app
and the compressed database, and an SSH control socket in `$XDG_RUNTIME_DIR` so that one SSH
connection serves a whole run.

## What was not tested

- A real Oracle VM: Oracle's own iptables rules (the test used a copy of the rules from Oracle's
  tutorial), the VCN security list, arm64, and the iptables rules surviving a reboot.
- A Let's Encrypt certificate. The test used a `.localhost` name, for which Caddy uses its own
  internal certificate authority.
- The real `/healthz`: the site skeleton has none yet, so the database tests used a stand-in app
  that answers `/healthz` from the database's `schema_version`.
- The swap file (the test machine had more than 2 GB of RAM).
- In the rootless container, systemd's sandboxing options needed `--cap-add SYS_ADMIN` (they use
  mount namespaces). On a VM, systemd runs as root and has this. If the site fails to start with
  status `226/NAMESPACE` or `217/USER`, the sandboxing options in `beastie-site.service` are the
  place to look.
