#!/usr/bin/env python3
"""Generate read-only discovery-yield feedback and optional priority weights."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

from cc_links.feedback import collect_pattern_feedback


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Measure postfetch yield by discovery pattern, tier and geo"
    )
    parser.add_argument("--db", required=True)
    parser.add_argument("--output")
    parser.add_argument(
        "--manifest", action="append", default=[],
        help="Legacy discovery JSONL to supply missing pattern/tier/geo attribution")
    parser.add_argument("--minimum-samples", type=int, default=20)
    parser.add_argument("--prior-strength", type=float, default=20.0)
    args = parser.parse_args()

    report = collect_pattern_feedback(
        args.db,
        minimum_samples=args.minimum_samples,
        prior_strength=args.prior_strength,
        manifest_paths=args.manifest,
    )
    encoded = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.output:
        Path(args.output).write_text(encoded + "\n", encoding="utf-8")
        print(f"[feedback] wrote {args.output}")
    summary = report["summary"]
    print(
        "[feedback] "
        f"decisions={summary['attributed_decisions']} "
        f"qualified={summary['qualified']} "
        f"rate={summary['baseline_qualified_rate']} "
        f"patterns={summary['patterns']} "
        f"unresolved_errors={summary['unresolved_fetch_errors']}"
    )
    if not args.output:
        print(encoded)


if __name__ == "__main__":
    main()
