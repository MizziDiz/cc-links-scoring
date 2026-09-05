#!/usr/bin/env python3
"""Report historical fetch yield per discovery pattern and write a yield profile.

Reads the SQLite database read-only. The profile feeds
``prospect_pipeline.py --pattern-min-yield ... --pattern-yield-file``; the
what-if table shows what a threshold would have skipped on the same history.

    python yield_report.py --db prospects.db --since 2026-07-25 \
        --out pattern-yield.json --thresholds 0.1 0.2 0.3
"""
import argparse

from cc_links.yield_gate import collect_pattern_yield, save_yield_profile


def what_if(profile, threshold, minimum_decisions):
    groups = profile["groups"].values()
    decisions = sum(g["decisions"] for g in groups)
    stored = sum(g["stored"] for g in groups)
    unmatched = decisions - stored
    low = [g for g in groups
           if g["decisions"] >= minimum_decisions and g["yield"] < threshold]
    return {
        "threshold": threshold,
        "groups": len(low),
        "fetches_avoided": sum(g["decisions"] for g in low),
        "fetches_total": decisions,
        "unmatched_avoided": sum(g["decisions"] - g["stored"] for g in low),
        "unmatched_total": unmatched,
        "stored_lost": sum(g["stored"] for g in low),
        "stored_total": stored,
        "stored_domains_lost_upper": sum(g["stored_domains"] for g in low),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--db", required=True)
    parser.add_argument("--since", help="Count rows processed at or after YYYY-MM-DD")
    parser.add_argument("--minimum-decisions", type=int, default=20)
    parser.add_argument("--thresholds", type=float, nargs="*", default=[0.1, 0.2, 0.3])
    parser.add_argument("--out", help="Write the yield profile JSON here")
    parser.add_argument("--top", type=int, default=15,
                        help="How many largest low-yield groups to list")
    args = parser.parse_args()

    profile = collect_pattern_yield(
        args.db, since=args.since, minimum_decisions=args.minimum_decisions)
    groups = sorted(profile["groups"].values(), key=lambda g: -g["decisions"])
    print(f"groups={len(groups)} decisions={sum(g['decisions'] for g in groups)} "
          f"stored={sum(g['stored'] for g in groups)} since={args.since or 'all'}")
    print()
    print(f"{'pattern|tier':40} {'decisions':>10} {'stored':>9} {'yield':>7} {'domains':>8}")
    for group in groups[:args.top]:
        print(f"{group['pattern_id'] + '|' + str(group['discovery_tier']):40} "
              f"{group['decisions']:>10} {group['stored']:>9} "
              f"{group['yield']:>7.1%} {group['stored_domains']:>8}")
    print()
    for threshold in args.thresholds:
        w = what_if(profile, threshold, args.minimum_decisions)
        share = lambda a, b: (a / b) if b else 0.0
        print(f"threshold<{threshold:.0%}: skip {w['groups']} groups, "
              f"fetches -{share(w['fetches_avoided'], w['fetches_total']):.1%}, "
              f"unmatched -{share(w['unmatched_avoided'], w['unmatched_total']):.1%}, "
              f"stored -{share(w['stored_lost'], w['stored_total']):.1%}, "
              f"stored domains (upper) -{w['stored_domains_lost_upper']}")
    if args.out:
        save_yield_profile(profile, args.out)
        print(f"\nprofile written to {args.out}")


if __name__ == "__main__":
    main()
