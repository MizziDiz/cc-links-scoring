#!/usr/bin/env python3
"""Self-healing gentle fetch supervisor.

Our single IP gets CloudFront-banned (403) if pushed, but tolerates a low rate.
Bans clear after a cooldown. This supervisor:
  1. waits until the IP is un-banned (robots.txt -> 200),
  2. starts a gentle fetch (low rate),
  3. watches the page count; if it stops growing (= re-banned / stalled),
     it kills the fetch, waits for the ban to clear, and restarts.
Runs until all candidates are processed. Fully resumable (--skip-discovery).
"""
import subprocess
import sqlite3
import sys
import time

import requests

DB = "latam.db"
RATE = "50"
WORKERS = "100"
# Optional rotating-gateway endpoint, kept out of the tracked code (credentials).
GATEWAY = ""
try:
    with open("gateway.txt", "r", encoding="utf-8") as _g:
        GATEWAY = _g.read().strip()
except OSError:
    pass
ROBOTS = "https://data.commoncrawl.org/robots.txt"
STALL_SECS = 300       # no page growth this long => assume banned
POLL_SECS = 120        # how often the watchdog checks growth
COOLDOWN_POLL = 60     # how often to re-check a banned IP

FETCH_CMD = [
    sys.executable, "-u", "pipeline.py", "countries",
    "--categories-file", "categories.json", "--per-category-limit", "100000",
    "--max-per-domain", "300", "--skip-discovery",
    "--candidates-file", "latam8h.candidates.jsonl",
    "--workers", WORKERS, "--rate-limit", RATE, "--no-links", "--db", DB,
]
if GATEWAY:
    FETCH_CMD += ["--proxy", GATEWAY]


def log(msg):
    print(f"[supervisor {time.strftime('%H:%M:%S')}] {msg}", flush=True)


def pages():
    try:
        c = sqlite3.connect(DB)
        n = c.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
        c.close()
        return n
    except Exception:
        return -1


def ip_ok():
    try:
        return requests.get(ROBOTS, timeout=10).status_code == 200
    except Exception:
        return False


def wait_until_unbanned():
    while not ip_ok():
        log("IP still banned/unreachable, waiting...")
        time.sleep(COOLDOWN_POLL)
    log("IP is clear (200)")


def main():
    while True:
        wait_until_unbanned()
        log(f"starting fetch at rate {RATE}/s (pages so far: {pages()})")
        proc = subprocess.Popen(FETCH_CMD, stdout=open("fetch.log", "a"),
                                stderr=subprocess.STDOUT)
        last_growth = time.time()
        last_count = pages()
        stalled = False
        while True:
            time.sleep(POLL_SECS)
            if proc.poll() is not None:
                log(f"fetch process exited on its own (code {proc.returncode}) -> all candidates done")
                log(f"FINAL pages: {pages()}")
                return
            n = pages()
            if n > last_count:
                last_count = n
                last_growth = time.time()
                log(f"progress: {n} pages")
            elif time.time() - last_growth > STALL_SECS:
                log(f"no growth for {STALL_SECS}s (pages={n}) -> likely re-banned, restarting")
                stalled = True
                break
        proc.terminate()
        try:
            proc.wait(timeout=30)
        except Exception:
            proc.kill()
        if stalled:
            time.sleep(30)  # small pause before re-checking the ban


if __name__ == "__main__":
    main()
