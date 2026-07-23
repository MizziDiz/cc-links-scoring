#!/usr/bin/env python3
"""Harvest + strictly validate free public HTTP proxies for CommonCrawl WARC fetching.

A proxy only survives if it can complete a real WARC byte-range request to
data.commoncrawl.org and return exactly the requested bytes (HTTP 206). Existing
entries in the output file are re-tested too, so running this repeatedly keeps
the file equal to the currently-working set (dead ones drop, new ones join).

    python harvest_proxies.py [out=proxies.txt] [max_test=4000]
"""
import concurrent.futures as cf
import json
import os
import random
import sys

import requests

SOURCES = [
    "https://raw.githubusercontent.com/TheSpeedX/PROXY-List/master/http.txt",
    "https://raw.githubusercontent.com/monosans/proxy-list/main/proxies/http.txt",
    "https://raw.githubusercontent.com/proxifly/free-proxy-list/main/proxies/protocols/http/data.txt",
    "https://raw.githubusercontent.com/clarketm/proxy-list/master/proxy-list-raw.txt",
    "https://raw.githubusercontent.com/ShiftyTR/Proxy-List/master/http.txt",
    "https://raw.githubusercontent.com/mmpx12/proxy-list/master/http.txt",
    "https://raw.githubusercontent.com/roosterkid/openproxylist/main/HTTPS_RAW.txt",
    "https://raw.githubusercontent.com/hookzof/socks5_list/master/proxy.txt",
]


def fetch_lists():
    seen = set()
    for url in SOURCES:
        try:
            txt = requests.get(url, timeout=25).text
        except Exception:
            continue
        for line in txt.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            line = line.split("//")[-1].split()[0]
            host, _, port = line.partition(":")
            if "." in host and port.isdigit():
                seen.add(f"{host}:{port}")
    return seen


def make_test():
    rec = json.loads(open("latam.db.candidates.jsonl").readline())
    fn, off, ln = rec["filename"], int(rec["offset"]), int(rec["length"])
    url = "https://data.commoncrawl.org/" + fn
    hdr = {"Range": f"bytes={off}-{off + ln - 1}"}

    def test(hostport):
        p = f"http://{hostport}"
        try:
            r = requests.get(url, headers=hdr, proxies={"http": p, "https": p}, timeout=12)
            if r.status_code == 206 and len(r.content) == ln:
                return hostport
        except Exception:
            return None
        return None

    return test


def main():
    out = sys.argv[1] if len(sys.argv) > 1 else "proxies.txt"
    max_test = int(sys.argv[2]) if len(sys.argv) > 2 else 4000

    cand = fetch_lists()
    if os.path.exists(out):  # re-test the ones we already trust
        for line in open(out):
            line = line.strip()
            if line:
                cand.add(line.split("://")[-1])
    cand = list(cand)
    random.shuffle(cand)
    cand = cand[:max_test]

    test = make_test()
    working = []
    with cf.ThreadPoolExecutor(max_workers=200) as ex:
        for res in ex.map(test, cand):
            if res:
                working.append(res)

    tmp = out + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write("\n".join(working) + ("\n" if working else ""))
    os.replace(tmp, out)
    print(f"[harvest] tested {len(cand)}, working {len(working)} -> {out}", flush=True)


if __name__ == "__main__":
    main()
