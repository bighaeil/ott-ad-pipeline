#!/usr/bin/env bash
# 실행 중인 Flink 잡을 전부 취소한다.
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
IDS=$(curl -fsS http://localhost:8181/jobs 2>/dev/null | tr '{' '\n' | grep '"status":"RUNNING"' | grep -o '"id":"[a-f0-9]*"' | cut -d'"' -f4)
if [[ -z "$IDS" ]]; then echo "[flink-cancel] 실행 중인 잡 없음"; exit 0; fi
for id in $IDS; do
  echo -n "[flink-cancel] $id ... "
  curl -fsS -X PATCH "http://localhost:8181/jobs/$id?mode=cancel" >/dev/null 2>&1 && echo "취소" || echo "실패"
done
