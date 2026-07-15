#!/usr/bin/env python3
"""Split a scored URL CSV/CSV.GZ into URL-only score bands."""
import argparse
import csv
import gzip


def main():
    p = argparse.ArgumentParser()
    p.add_argument("input")
    p.add_argument("--prefix", default="latam_urls")
    args = p.parse_args()

    opener = gzip.open if args.input.lower().endswith(".gz") else open
    paths = {
        "above_50": f"{args.prefix}_above_50.txt",
        "30_to_50": f"{args.prefix}_30_to_50.txt",
        "below_30": f"{args.prefix}_below_30.txt",
    }
    counts = {key: 0 for key in paths}
    outputs = {key: open(path, "w", encoding="utf-8", newline="\n")
               for key, path in paths.items()}
    try:
        with opener(args.input, "rt", encoding="utf-8", newline="") as src:
            for row in csv.DictReader(src):
                score = int(row["score"])
                key = "above_50" if score > 50 else "30_to_50" if score >= 30 else "below_30"
                outputs[key].write(row["url"] + "\n")
                counts[key] += 1
    finally:
        for out in outputs.values():
            out.close()

    for key, path in paths.items():
        print(f"{key}: {counts[key]} -> {path}")


if __name__ == "__main__":
    main()
