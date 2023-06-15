#!/bin/bash

set -o pipefail
set -e

pyenv local 3.8.16 3.9.12 3.10.4 3.11.1 \
&& poetry lock \
&& poetry install
