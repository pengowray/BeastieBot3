#!/usr/bin/env python3
"""Round-trip check of the /update page on real wikitext.

Sends each .txt file in a folder to a running site's /update page twice, with several sets of
options, in LF and in CRLF, and reports every page whose second run changes the text again, whose
CRLF output has a bare LF or "\\r\\r", or that takes more than 1.5 s. A second run must change
nothing: every status it added is up to date and every taxon it put in is listed.

    python3 update-roundtrip.py <folder of .txt pages> [site URL, default http://127.0.0.1:5083]

The site must allow many updates a minute (Site__RateLimits__UpdatesPerMinute). Wikitext of
articles can be read from the Wikipedia cache (wiki_pages.wikitext) or with action=raw.
"""
import glob
import html
import re
import subprocess
import sys
import tempfile
import time

OPTION_SETS = [
    ["addlines", "addcols", "addmissing", "ids", "year"],
    ["addlines", "addcols", "addmissing", "addrefs", "addend", "extra"],
]


def run(url, text, options):
    with tempfile.NamedTemporaryFile("w", suffix=".txt", newline="", encoding="utf-8", delete=False) as f:
        f.write(text)
        path = f.name
    args = ["curl", "-s", "-F", "text=<" + path] + sum([["-F", o + "=1"] for o in options], []) + [url + "/update"]
    started = time.time()
    page = subprocess.run(args, capture_output=True).stdout.decode("utf-8")
    seconds = time.time() - started
    m = re.search(r'id="update-output"[^>]*>(.*?)</textarea>', page, re.S)
    # The textarea starts with a newline that the page writes before the text.
    return (html.unescape(m.group(1))[1:] if m else None), seconds


def main():
    folder = sys.argv[1]
    url = sys.argv[2] if len(sys.argv) > 2 else "http://127.0.0.1:5083"
    problems = 0
    files = sorted(glob.glob(folder + "/*.txt"))
    for options in OPTION_SETS:
        for crlf in (False, True):
            for path in files:
                text = open(path, encoding="utf-8", newline="").read().replace("\r\n", "\n")
                if crlf:
                    text = text.replace("\n", "\r\n")
                first, s1 = run(url, text, options)
                second, s2 = run(url, first, options) if first is not None else (None, 0)
                name = path.split("/")[-1]
                label = f"{'+'.join(options)} {'CRLF' if crlf else 'LF'} {name}"
                if max(s1, s2) > 1.5:
                    print(f"SLOW {max(s1, s2):.2f} s: {label}")
                if first is None or second is None:
                    problems += 1
                    print("NO OUTPUT:", label)
                elif first != second:
                    problems += 1
                    print("SECOND RUN CHANGES THE TEXT:", label)
                elif crlf and ("\r\r" in first or "\n" in first.replace("\r\n", "")):
                    problems += 1
                    print("MIXED LINE ENDS:", label)
    print(f"{len(files)} pages, {problems} problems")
    sys.exit(1 if problems else 0)


if __name__ == "__main__":
    main()
