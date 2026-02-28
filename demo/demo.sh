#!/usr/bin/env bash
# s3fetch demo script — used with:
#   asciinema rec demo/demo.cast -c ./demo/demo.sh \
#     --title "s3fetch demo" --cols 100 --rows 30 -i 2 --overwrite

BUCKET="s3://s3fetch-cicd-test-bucket"
DOWNLOAD_DIR="/tmp/s3fetch-demo"

mkdir -p "$DOWNLOAD_DIR"

# Print a fake prompt line then run the command
_run() {
    echo "$ $*"
    eval "$@"
}

sleep 1

# ── Step 1: dry-run (list-only mode) ─────────────────────────────────────────
_run s3fetch "$BUCKET/logs/" --dry-run
sleep 2

# ── Step 2: regex filter ─────────────────────────────────────────────────────
echo ""
_run s3fetch "$BUCKET/logs/" --regex "'2024-01-0[1-5]\.txt\$'" --progress detailed --download-dir "$DOWNLOAD_DIR"
sleep 2

# ── Step 3: single-threaded download ─────────────────────────────────────────
echo ""
_run s3fetch "$BUCKET/medium/" --regex "'data_00[0-9]\.jpg\$'" --threads 1 --progress detailed --download-dir "$DOWNLOAD_DIR"
sleep 2

# ── Step 4: multi-threaded download with fancy progress bar ──────────────────
echo ""
_run s3fetch "$BUCKET/medium/" --regex "'data_00[0-9]\.jpg\$'" --threads 10 --progress fancy --download-dir "$DOWNLOAD_DIR"
sleep 2
