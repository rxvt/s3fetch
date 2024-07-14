#!/bin/bash

set -o pipefail
set -e

# Fix git unsafe directory error as git dir is owned by `vscode` inside container
# https://github.com/microsoft/vscode-remote-release/issues/6810#issuecomment-1310980232
git config --global --add safe.directory "${CONTAINER_WORKSPACE_FOLDER}"
