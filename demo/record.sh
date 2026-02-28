#!/usr/bin/env bash
# Record the s3fetch demo using asciinema and convert to an animated GIF.
#
# Dependencies:
#   - asciinema  (https://asciinema.org/docs/installation)
#   - agg        (https://github.com/asciinema/agg/releases)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Check dependencies
for cmd in asciinema agg; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: '$cmd' is not installed or not in PATH." >&2
    echo "  asciinema: https://asciinema.org/docs/installation" >&2
    echo "  agg:       https://github.com/asciinema/agg/releases" >&2
    exit 1
  fi
done

echo "Recording demo..."
asciinema rec "$SCRIPT_DIR/demo.cast" \
  --command "$SCRIPT_DIR/demo.sh" \
  --title "s3fetch demo" \
  --cols 100 \
  --rows 35 \
  --idle-time-limit 2 \
  --overwrite

echo "Converting to GIF..."
agg \
  --cols 100 \
  --rows 35 \
  --idle-time-limit 2 \
  "$SCRIPT_DIR/demo.cast" \
  "$SCRIPT_DIR/demo.gif"

echo "Done! Files written:"
echo "  $SCRIPT_DIR/demo.cast"
echo "  $SCRIPT_DIR/demo.gif"
