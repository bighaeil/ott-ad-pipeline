#!/usr/bin/env bash
# 트래픽 생성기 실행.
#   MODE=normal|live|burst  USERS=200  DURATION=60
#   추가 인자는 그대로 생성기에 전달된다.
#     bash scripts/load.sh --late 0.3 --ssai 0.1
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

MODE="${MODE:-normal}"
USERS="${USERS:-200}"
DURATION="${DURATION:-60}"

echo "[load] mode=$MODE users=$USERS duration=${DURATION}s"
exec docker compose --profile load run --rm --no-deps generator \
  --mode "$MODE" --users "$USERS" --duration "$DURATION" "$@"
