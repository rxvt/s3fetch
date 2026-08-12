#!/usr/bin/env bash
set -euo pipefail

version="${1:?Usage: release.sh <version>}"

if ! gh auth status >/dev/null 2>&1; then
  echo "Not authenticated with GitHub CLI. Run: gh auth login"
  exit 1
fi

if [ -n "$(git status --porcelain)" ]; then
  echo "Working tree is not clean. Commit or stash changes before releasing."
  exit 1
fi

just check

if [ -n "${SKIP_TESTS:-}" ]; then
  echo "SKIP_TESTS is set — skipping check and test suite"
else
  just test -a
fi

hatch version "${version}"
git add src/s3fetch/__init__.py
git commit -m "Bump version to ${version}"
git tag -a "v${version}" -m "Release v${version}"
git push origin HEAD
git push origin "v${version}"
gh release create "v${version}" --title "v${version}" --generate-notes
