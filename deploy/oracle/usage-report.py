#!/usr/bin/env python3
"""Usage report for the public site, from Caddy's JSON access logs.

usage.sh sends this script to the server and runs it there as root (the logs are readable by the
caddy user only). It reads /var/log/caddy/access.log and the rolled files beside it (Caddy keeps
them for 14 days, gzipped or not), and prints counts only: no IP addresses and no user agents of
people. Standard library only, so the server needs nothing installed.

  python3 usage-report.py [--days N] [--top N] [--log-dir DIR]
"""
import argparse
import collections
import datetime as dt
import glob
import gzip
import json
import os
import re
import sys
from urllib.parse import parse_qs, urlsplit

STATIC = re.compile(r"\.(css|js|png|ico|svg|webmanifest|txt|map)$", re.I)
BOT = re.compile(r"bot|crawl|spider|slurp|fetch|scan|curl|wget|python|go-http|java/|okhttp|httpclient|"
                 r"headless|lighthouse|monitor|preview|facebookexternalhit|bingpreview|semrush|ahrefs|"
                 r"petal|bytespider|gptbot|claude|perplexity|ccbot|dataforseo|forestengine", re.I)
# Requests for files the site has never had: scanners looking for secrets and admin pages.
PROBE = re.compile(r"/\.|\.env|\.php|wp-|/admin|/cgi-bin|\.\./", re.I)
BOT_NAME = re.compile(r"([A-Za-z][\w.-]*(?:bot|crawler|spider|Engine))", re.I)


def log_files(log_dir):
    files = glob.glob(os.path.join(log_dir, "access*.log*"))
    return sorted(files, key=os.path.getmtime)


def entries(files):
    for path in files:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8", errors="replace") as f:
            for line in f:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue


def client(e):
    request = e.get("request") or {}
    return request.get("client_ip") or request.get("remote_ip") or ""


def header(request, name):
    values = (request.get("headers") or {}).get(name) or []
    return values[0] if values else ""


def page_kind(path):
    if path == "/":
        return "home"
    for prefix, kind in (("/species/", "taxon"), ("/taxa/", "group"), ("/search", "search"),
                         ("/name/", "name"), ("/update", "update"), ("/about", "about"),
                         ("/api/suggest", "suggest"), ("/error", "error")):
        if path.startswith(prefix):
            return kind
    return "other"


def table(rows, headings):
    widths = [max(len(str(r[i])) for r in [headings] + rows) for i in range(len(headings))]
    def line(r):
        return "  ".join(str(v).rjust(w) if i > 0 else str(v).ljust(w) for i, (v, w) in enumerate(zip(r, widths)))
    print(line(headings))
    for r in rows:
        print(line(r))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--top", type=int, default=15)
    parser.add_argument("--log-dir", default="/var/log/caddy")
    args = parser.parse_args()

    since = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=args.days)
    days = collections.defaultdict(lambda: collections.Counter())
    visitors = collections.defaultdict(set)
    taxa = collections.Counter()
    groups = collections.Counter()
    searches = collections.Counter()
    referrers = collections.Counter()
    bots = collections.Counter()
    kinds = collections.Counter()
    refused = collections.Counter()
    first = last = None

    files = log_files(args.log_dir)
    if not files:
        sys.exit(f"No access logs in {args.log_dir}")
    recent = [e for e in entries(files) if dt.datetime.fromtimestamp(e.get("ts", 0), dt.timezone.utc) >= since]
    # An address that hit the rate limit on a day is a script that day, whatever its user agent: a
    # scraper with an ordinary Chrome user agent walked the group pages on 2026-10-05 and was
    # refused 148 times.
    limited = {(dt.datetime.fromtimestamp(e["ts"], dt.timezone.utc).date().isoformat(), client(e))
               for e in recent if e.get("status") == 429}
    for e in recent:
        ts = dt.datetime.fromtimestamp(e.get("ts", 0), dt.timezone.utc)
        request = e.get("request") or {}
        status = e.get("status", 0)
        location = ((e.get("resp_headers") or {}).get("Location") or [""])[0]
        if status == 308 and location.startswith("https://"):
            continue  # Caddy's http:// -> https:// redirect
        split = urlsplit(request.get("uri", ""))
        path = split.path
        if path == "/healthz" or STATIC.search(path):
            continue
        first = ts if first is None or ts < first else first
        last = ts if last is None or ts > last else last
        day = ts.date().isoformat()
        agent = header(request, "User-Agent")
        if (day, client(e)) in limited:
            days[day]["bot requests"] += 1
            bots["(addresses that hit the rate limit that day)"] += 1
            if status == 429:
                days[day]["429 (scripts)"] += 1
            continue
        if PROBE.search(path):
            days[day]["bot requests"] += 1
            bots["(probes for .env, .git, PHP and admin pages)"] += 1
            continue
        if not agent or BOT.search(agent):
            days[day]["bot requests"] += 1
            match = BOT_NAME.search(agent)
            name = match.group(1) if match else (agent.split("/")[0][:40] or "(no user agent)")
            bots[name] += 1
            continue
        kind = page_kind(path)
        method = request.get("method", "GET")
        if status >= 500:
            days[day]["5xx"] += 1
            refused[(status, kind)] += 1
        if status == 429:
            days[day]["429"] += 1
            refused[(status, kind)] += 1
        if kind == "suggest":
            days[day]["suggest calls"] += 1
            continue
        if method == "POST" and kind == "update":
            days[day]["update runs"] += 1
            kinds["update run"] += 1
            continue
        if status >= 400:
            continue
        days[day]["page views"] += 1
        kinds[kind] += 1
        visitors[day].add(client(e))
        if kind == "taxon":
            taxa[path.split("/")[2]] += 1
        elif kind == "group":
            groups[path] += 1
        elif kind == "search":
            q = parse_qs(split.query).get("q", [""])[0].strip()
            if q:
                searches[q.lower()] += 1
                days[day]["searches"] += 1
        ref = header(request, "Referer")
        if ref:
            host = urlsplit(ref).netloc
            if host and host != request.get("host"):
                referrers[host] += 1

    if first is None:
        print(f"No requests in the last {args.days} days.")
        return
    print(f"Requests from {first:%Y-%m-%d %H:%M} to {last:%Y-%m-%d %H:%M} UTC (last {args.days} days asked for).")
    print("People: requests without a crawler or script user agent, from addresses that did not hit the rate limit that day.")
    print("Visitors: distinct addresses of people per day (counted, not shown). 429 (scripts): refusals of the addresses that hit the rate limit.")
    print()
    cols = ["page views", "visitors", "searches", "update runs", "suggest calls", "bot requests", "429", "429 (scripts)", "5xx"]
    rows = []
    for day in sorted(days):
        c = days[day]
        rows.append([day] + [len(visitors[day]) if k == "visitors" else c[k] for k in cols])
    table(rows, ["Day (UTC)"] + cols)

    def top(title, counter, label):
        if not counter:
            return
        print()
        print(title)
        table([[k, v] for k, v in counter.most_common(args.top)], [label, "Count"])

    top("Page views by page type (people)", kinds, "Page type")
    top("Errors and rate limits by status and page type (people)",
        collections.Counter({f"{st} {k}": n for (st, k), n in refused.items()}), "Status and page type")
    top("Most viewed taxon pages (IUCN taxon id)", taxa, "Taxon id")
    top("Most viewed group pages", groups, "Page")
    top("Most frequent searches", searches, "Search text")
    top("Referring sites", referrers, "Site")
    top("Crawlers and scripts", bots, "User agent")


if __name__ == "__main__":
    main()
