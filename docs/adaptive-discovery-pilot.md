# Adaptive discovery pilot

This pilot checks whether post-fetch pattern feedback improves useful yield at
a fixed WARC-fetch budget. The adaptive path remains optional; the default
queue order and final scoring threshold are unchanged.

## Method

- Crawl: `CC-MAIN-2026-25`.
- Source: same-region S3 range reads on a 1-vCPU EC2 instance.
- Input: one completed broad-discovery manifest with 6,034 URLs.
- Split: stable URL-hash split, 4,246 train and 1,788 held-out test URLs.
- Training labels: 4,222 attributed post-fetch decisions from the train split.
- Test budget: first 750 new URLs from the same held-out manifest in each arm.
- Both arms used the same taxonomy, `--min-score 50`, 64 fetch workers and
  `--max-per-domain 10`.
- Control used the existing tier/prefetch ordering. Adaptive additionally used
  `pattern-priorities.json` generated only from the train split.

No Parquet discovery was repeated for the A/B test. Legacy `pattern_id` values
were derived from each manifest URL in memory.

## Results

| Metric | Control | Adaptive |
|---|---:|---:|
| Fetched decisions | 750 | 750 |
| Qualified (`stored`) | 657 | 750 |
| Unmatched | 93 | 0 |
| Qualified rate | 87.6% | 100.0% |
| Candidate domains | 425 | 363 |
| Maximum URLs/domain | 5 | 6 |
| Mean final score | 78.72 | 78.25 |
| Wall time | 5.826 s | 5.207 s |

Adaptive scheduling produced 93 additional classified candidates at the same
fetch budget, a 14.2% increase over control. It concentrated more pages on
already productive domains, while remaining below the hard cap of 10.

## Read-only live validation

Stratified samples were fetched with GET requests only. No forms were
submitted.

| Metric | Control | Adaptive |
|---|---:|---:|
| Sample rows | 67 | 52 |
| HTTP 2xx | 58 (86.6%) | 50 (96.2%) |
| Current classifier match | 61 (91.0%) | 52 (100.0%) |
| Family agreement when comparable | 100.0% | 98.1% |

## Interpretation and limits

The held-out result supports using learned pattern weights to order fetches,
but it is not a universal precision estimate. It covers one crawl, one machine
and a small stratified live sample. The next production-sized run should keep
the feature opt-in, preserve an exploration floor for patterns with fewer than
20 decisions, and regenerate weights from newly observed outcomes. Final HTML
classification, minimum score and the 10-URL domain cap remain authoritative.
