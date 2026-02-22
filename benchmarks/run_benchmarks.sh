#!/usr/bin/env bash
# =============================================================================
# Benchmark s3fetch against other S3 download tools
#
# Compares: s3fetch, AWS CLI v2, s5cmd, s3cmd, rclone
# Uses:     hyperfine (https://github.com/sharkdp/hyperfine)
# Output:   BENCHMARKS.md in the repo root
#
# Prerequisites — install before running:
#   hyperfine : sudo apt install hyperfine  OR  cargo install hyperfine
#   s3fetch   : pip install -e .  (from repo root)
#   aws cli   : https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html
#   s5cmd     : https://github.com/peak/s5cmd/releases  (download binary)
#   s3cmd     : pip install s3cmd  OR  sudo apt install s3cmd
#   rclone    : curl https://rclone.org/install.sh | sudo bash
#
# Rclone one-time setup (uses standard AWS credential chain automatically):
#   rclone config create s3bench s3 provider AWS region us-east-1
#
# AWS credentials:
#   Standard boto3/AWS credential chain is used by all tools.
#   Set AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY as needed,
#   or rely on ~/.aws/credentials / EC2 instance role.
#
# Usage:
#   cd <repo-root>
#   bash benchmarks/run_benchmarks.sh
# =============================================================================

set -euo pipefail

# Ensure uv-installed tools (s3fetch, s3cmd) are on PATH
export PATH="$HOME/.local/bin:$PATH"

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
BUCKET="s3fetch-cicd-test-bucket"
REGION="us-east-1"
RUNS=3
BENCH_DIR="/tmp/s3fetch_bench"
OUTPUT_MD="BENCHMARKS.md"

# Rclone remote name — must match what you configured with `rclone config create`
RCLONE_REMOTE="s3bench"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
die() { echo "ERROR: $*" >&2; exit 1; }

check_tool() {
    local tool="$1"
    if ! command -v "$tool" &>/dev/null; then
        die "'$tool' not found on PATH. See prerequisites at the top of this script."
    fi
}

tool_version() {
    local tool="$1"
    case "$tool" in
        s3fetch)  s3fetch --version 2>/dev/null || echo "unknown" ;;
        aws)      aws --version 2>/dev/null | head -1 || echo "unknown" ;;
        s5cmd)    s5cmd version 2>/dev/null | head -1 || echo "unknown" ;;
        s3cmd)    s3cmd --version 2>/dev/null | head -1 || echo "unknown" ;;
        rclone)   rclone version 2>/dev/null | head -1 || echo "unknown" ;;
        hyperfine) hyperfine --version 2>/dev/null || echo "unknown" ;;
    esac
}

prepare_cmd() {
    echo "rm -rf '${BENCH_DIR}' && mkdir -p '${BENCH_DIR}'"
}

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------
for tool in hyperfine s3fetch aws s5cmd s3cmd rclone; do
    check_tool "$tool"
done

echo "All required tools found."
echo ""

# ---------------------------------------------------------------------------
# Build BENCHMARKS.md header
# ---------------------------------------------------------------------------
cat > "$OUTPUT_MD" <<EOF
# s3fetch Benchmarks

Comparing s3fetch against AWS CLI v2, s5cmd, s3cmd, and rclone for S3 download
workloads across three scenarios.

**Generated:** $(date -u '+%Y-%m-%d %H:%M UTC')
**Runs per command:** ${RUNS} (mean ± stddev reported by hyperfine)
**S3 bucket:** \`s3://${BUCKET}\` (us-east-1)

## Tool Versions

| Tool | Version |
|------|---------|
| s3fetch | $(tool_version s3fetch) |
| aws cli | $(tool_version aws) |
| s5cmd | $(tool_version s5cmd) |
| s3cmd | $(tool_version s3cmd) |
| rclone | $(tool_version rclone) |
| hyperfine | $(tool_version hyperfine) |

## Environment

<!-- Fill in manually after running: -->
<!-- Machine: e.g. EC2 t3.medium, us-east-1 / local laptop + home broadband -->
<!-- OS: $(uname -srm) -->
<!-- CPU: $(nproc) vCPUs -->

---

EOF

echo "==================================================================="
echo " SCENARIO 1: Many small files (small/, 120 × small .txt files)"
echo "==================================================================="
echo ""

cat >> "$OUTPUT_MD" <<'EOF'
## Scenario 1: Many small files

**Prefix:** `small/` — 120 small `.txt` files
**Tests:** Concurrency / object-listing throughput

### Default concurrency settings

EOF

hyperfine \
    --runs "$RUNS" \
    --warmup 0 \
    --prepare "$(prepare_cmd)" \
    --export-markdown /tmp/bench_s1_default.md \
    --command-name "s3fetch (default threads)" \
        "s3fetch s3://${BUCKET}/small/ --download-dir ${BENCH_DIR} --region ${REGION} --quiet" \
    --command-name "aws cli" \
        "aws s3 cp s3://${BUCKET}/small/ ${BENCH_DIR} --recursive --region ${REGION} --quiet" \
    --command-name "s5cmd (256 workers)" \
        "s5cmd cp 's3://${BUCKET}/small/*' ${BENCH_DIR}/" \
    --command-name "s3cmd" \
        "s3cmd get --recursive s3://${BUCKET}/small/ ${BENCH_DIR}/ --quiet" \
    --command-name "rclone (4 transfers)" \
        "rclone copy ${RCLONE_REMOTE}:${BUCKET}/small/ ${BENCH_DIR}/ --quiet"

cat /tmp/bench_s1_default.md >> "$OUTPUT_MD"

cat >> "$OUTPUT_MD" <<'EOF'

### Standardised 10-thread/transfer settings

> s3cmd is single-threaded and is excluded from this pass.

EOF

hyperfine \
    --runs "$RUNS" \
    --warmup 0 \
    --prepare "$(prepare_cmd)" \
    --export-markdown /tmp/bench_s1_tuned.md \
    --command-name "s3fetch --threads 10" \
        "s3fetch s3://${BUCKET}/small/ --download-dir ${BENCH_DIR} --region ${REGION} --threads 10 --quiet" \
    --command-name "aws cli" \
        "aws s3 cp s3://${BUCKET}/small/ ${BENCH_DIR} --recursive --region ${REGION} --quiet" \
    --command-name "s5cmd --numworkers 10" \
        "s5cmd --numworkers 10 cp 's3://${BUCKET}/small/*' ${BENCH_DIR}/" \
    --command-name "rclone --transfers 10" \
        "rclone copy ${RCLONE_REMOTE}:${BUCKET}/small/ ${BENCH_DIR}/ --transfers 10 --quiet"

cat /tmp/bench_s1_tuned.md >> "$OUTPUT_MD"

echo ""
echo "==================================================================="
echo " SCENARIO 2: Medium-sized files (medium/, 80 × 1-10 MB .jpg files)"
echo "==================================================================="
echo ""

cat >> "$OUTPUT_MD" <<'EOF'

---

## Scenario 2: Medium-sized files

**Prefix:** `medium/` — 80 files, 1–10 MB each
**Tests:** Parallel download throughput

### Default concurrency settings

EOF

hyperfine \
    --runs "$RUNS" \
    --warmup 0 \
    --prepare "$(prepare_cmd)" \
    --export-markdown /tmp/bench_s2_default.md \
    --command-name "s3fetch (default threads)" \
        "s3fetch s3://${BUCKET}/medium/ --download-dir ${BENCH_DIR} --region ${REGION} --quiet" \
    --command-name "aws cli" \
        "aws s3 cp s3://${BUCKET}/medium/ ${BENCH_DIR} --recursive --region ${REGION} --quiet" \
    --command-name "s5cmd (256 workers)" \
        "s5cmd cp 's3://${BUCKET}/medium/*' ${BENCH_DIR}/" \
    --command-name "s3cmd" \
        "s3cmd get --recursive s3://${BUCKET}/medium/ ${BENCH_DIR}/ --quiet" \
    --command-name "rclone (4 transfers)" \
        "rclone copy ${RCLONE_REMOTE}:${BUCKET}/medium/ ${BENCH_DIR}/ --quiet"

cat /tmp/bench_s2_default.md >> "$OUTPUT_MD"

cat >> "$OUTPUT_MD" <<'EOF'

### Standardised 10-thread/transfer settings

> s3cmd is single-threaded and is excluded from this pass.

EOF

hyperfine \
    --runs "$RUNS" \
    --warmup 0 \
    --prepare "$(prepare_cmd)" \
    --export-markdown /tmp/bench_s2_tuned.md \
    --command-name "s3fetch --threads 10" \
        "s3fetch s3://${BUCKET}/medium/ --download-dir ${BENCH_DIR} --region ${REGION} --threads 10 --quiet" \
    --command-name "aws cli" \
        "aws s3 cp s3://${BUCKET}/medium/ ${BENCH_DIR} --recursive --region ${REGION} --quiet" \
    --command-name "s5cmd --numworkers 10" \
        "s5cmd --numworkers 10 cp 's3://${BUCKET}/medium/*' ${BENCH_DIR}/" \
    --command-name "rclone --transfers 10" \
        "rclone copy ${RCLONE_REMOTE}:${BUCKET}/medium/ ${BENCH_DIR}/ --transfers 10 --quiet"

cat /tmp/bench_s2_tuned.md >> "$OUTPUT_MD"

echo ""
echo "==================================================================="
echo " SCENARIO 3: Regex/filter — only .json files from extensions/"
echo "==================================================================="
echo ""

cat >> "$OUTPUT_MD" <<'EOF'

---

## Scenario 3: Regex / filter (s3fetch differentiator)

**Prefix:** `extensions/` — mixed file types; download only `.json` files
**Tests:** Native pattern filtering without a multi-step shell pipeline

> **Key differentiator:** Only s3fetch and rclone natively support filtering
> without requiring a multi-step shell pipeline. s5cmd uses glob patterns
> (not full regex). aws cli and s3cmd have no native filter support and are
> excluded from this scenario.

EOF

hyperfine \
    --runs "$RUNS" \
    --warmup 0 \
    --prepare "$(prepare_cmd)" \
    --export-markdown /tmp/bench_s3.md \
    --command-name "s3fetch --regex '.*\\.json\$'" \
        "s3fetch s3://${BUCKET}/extensions/ --regex '.*\.json\$' --download-dir ${BENCH_DIR} --region ${REGION} --quiet" \
    --command-name "rclone --filter '+ *.json'" \
        "rclone copy ${RCLONE_REMOTE}:${BUCKET}/extensions/ ${BENCH_DIR}/ --filter '+ *.json' --filter '- *' --quiet" \
    --command-name "s5cmd (glob *.json — not full regex)" \
        "s5cmd cp 's3://${BUCKET}/extensions/*.json' ${BENCH_DIR}/"

cat /tmp/bench_s3.md >> "$OUTPUT_MD"

# ---------------------------------------------------------------------------
# Feature comparison table
# ---------------------------------------------------------------------------
cat >> "$OUTPUT_MD" <<'EOF'

---

## Feature Comparison

| Feature | s3fetch | aws cli | s5cmd | s3cmd | rclone |
|---------|:-------:|:-------:|:-----:|:-----:|:------:|
| Regex filtering | ✅ native | ❌ | ⚠️ glob only | ❌ | ⚠️ include patterns |
| Multi-threaded downloads | ✅ | ✅ | ✅ | ❌ | ✅ |
| Python installable (`pip`) | ✅ | ✅ | ❌ Go binary | ✅ | ❌ Go binary |
| Programmatic Python API | ✅ | ❌ | ❌ | ❌ | ❌ |
| Dry-run mode | ✅ | ✅ | ✅ | ✅ | ✅ |
| Early streaming (download while listing) | ✅ | ❌ | ❌ | ❌ | ❌ |

---

_Generated by `benchmarks/run_benchmarks.sh`_
EOF

echo ""
echo "==================================================================="
echo " Done! Results written to: ${OUTPUT_MD}"
echo "==================================================================="
