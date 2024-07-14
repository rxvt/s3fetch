#!/bin/bash

set -o pipefail
set -e

# Fix git unsafe directory error as git dir is owned by `vscode` inside container
# https://github.com/microsoft/vscode-remote-release/issues/6810#issuecomment-1310980232
git config --global --add safe.directory "${CONTAINER_WORKSPACE_FOLDER}"

# Remove SSH program configuration otherwise git inside the container will try to access the path for
# the SSH program from the host machine. This causes problems specifically with the 1Password MacOS integration.
git config --global --unset gpg.ssh.program

# The following commands can't be run during the docker build process as the source directory is not mounted yet,
# so we run them in the postCreateCommand instead.
# pre-commit install --install-hooks
hatch run default:pre-commit install --install-hooks
