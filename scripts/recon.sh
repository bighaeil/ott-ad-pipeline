#!/usr/bin/env bash
# 대사(reconciliation) 잡.
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."
exec bash scripts/batch.sh reconcile.py
