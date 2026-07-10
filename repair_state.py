#!/usr/bin/env python3
"""Un-mark the parts that FAILED (were throttled) in the previous discovery run so
a resume retries them. The old code marked throttled parts as 'scanned', which
permanently discarded everything in them. Parse discovery.log for the failures,
strip them from state.json's scanned_parts, keep the successful ones.
"""
import json
import re
import sys

LOG = "discovery.log"
STATE = "latam.db.candidates.jsonl.state.json"

failed = set()
for line in open(LOG, encoding="utf-8", errors="replace"):
    m = re.search(r"skip part (\d+)", line)
    if m:
        failed.add(int(m.group(1)))

s = json.load(open(STATE, encoding="utf-8"))
before = set(s["scanned_parts"])
after = sorted(before - failed)
s["scanned_parts"] = after
json.dump(s, open(STATE, "w", encoding="utf-8"))

print(f"failed parts in log : {len(failed)}")
print(f"scanned_parts       : {len(before)} -> {len(after)}")
print(f"will be retried on resume: {len(before) - len(after)}")
if not failed:
    print("WARNING: no 'skip part' lines found -- nothing to repair", file=sys.stderr)
