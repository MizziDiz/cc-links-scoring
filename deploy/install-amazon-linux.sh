#!/usr/bin/env bash
set -euo pipefail

RELEASE_TAG="${RELEASE_TAG:-prospects-v0.1.2}"
REPO_URL="${REPO_URL:-https://github.com/MizziDiz/cc-links-scoring.git}"
APP_DIR="${APP_DIR:-/opt/cc-links-scoring}"
DATA_DIR="${DATA_DIR:-/var/lib/cc-prospects}"

if command -v dnf >/dev/null 2>&1; then
  sudo dnf install -y git python3 python3-pip
else
  sudo yum install -y git python3 python3-pip
fi
if [[ ! -d "$APP_DIR/.git" ]]; then
  sudo git clone --branch "$RELEASE_TAG" --depth 1 "$REPO_URL" "$APP_DIR"
else
  sudo git -C "$APP_DIR" fetch --tags origin
  sudo git -C "$APP_DIR" checkout --detach "$RELEASE_TAG"
fi
sudo chown -R ec2-user:ec2-user "$APP_DIR"
sudo install -d -o ec2-user -g ec2-user "$DATA_DIR"

python3 -m venv "$APP_DIR/.venv"
"$APP_DIR/.venv/bin/pip" install --upgrade pip
"$APP_DIR/.venv/bin/pip" install -r "$APP_DIR/requirements.txt"

sudo install -m 0644 "$APP_DIR/deploy/cc-prospects.service" /etc/systemd/system/cc-prospects.service
sudo systemctl daemon-reload
sudo systemctl enable cc-prospects.service
sudo systemctl restart cc-prospects.service

echo "Collector installed. Follow progress with:"
echo "  sudo journalctl -fu cc-prospects.service"
