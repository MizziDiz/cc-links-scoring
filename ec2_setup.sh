#!/usr/bin/env bash
# One-shot setup for running the CommonCrawl pipeline on an EC2 instance
# (Amazon Linux 2023, us-east-1). Installs deps into a venv. Idempotent.
#
#   curl -fsSL <raw-url>/ec2_setup.sh | bash      # or copy this file up and run it
#
# After it finishes, run discovery then the S3 fetch (see the printed hints).
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/MizziDiz/cc-links-scoring.git}"
REPO_DIR="${REPO_DIR:-$HOME/cc-links-scoring}"

echo "== installing system packages =="
sudo dnf install -y python3 python3-pip git >/dev/null

echo "== cloning / updating repo =="
if [ -d "$REPO_DIR/.git" ]; then
  git -C "$REPO_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$REPO_DIR"
fi
cd "$REPO_DIR"

echo "== python venv + deps =="
python3 -m venv .venv
./.venv/bin/pip install --upgrade pip >/dev/null
./.venv/bin/pip install -r requirements.txt >/dev/null

echo "== sanity: can this instance read s3://commoncrawl ? =="
./.venv/bin/python - <<'PY'
import boto3
s3 = boto3.client("s3", region_name="us-east-1")
# This index-paths file exists for the crawl we scan; proves signed S3 read works.
r = s3.get_object(Bucket="commoncrawl",
                  Key="crawl-data/CC-MAIN-2026-25/cc-index-table.paths.gz",
                  Range="bytes=0-10")
print("  S3 read OK:", len(r["Body"].read()), "bytes -> the IAM role works")
PY

cat <<EOF

== setup done ==
Next, from $REPO_DIR:

1) Discovery (index scan via CloudFront, paced ~15-20 min):
   ./.venv/bin/python pipeline.py countries \\
       --categories-file categories.json --per-category-limit 100000 \\
       --max-per-domain 300 --discover-delay 2 --discovery-only --db latam.db

2) Fetch from S3 (no throttle, no proxy):
   ./.venv/bin/python pipeline.py countries \\
       --categories-file categories.json --per-category-limit 100000 \\
       --max-per-domain 300 --skip-discovery --source s3 \\
       --workers 64 --no-links --db latam.db

The result is latam.db (SQLite). Download it, then TERMINATE the instance.
EOF
