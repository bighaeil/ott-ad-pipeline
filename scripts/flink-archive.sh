#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 원본 Parquet 적재 잡 제출 (MinIO).
#
#   bash scripts/flink-archive.sh
#   ROLLOVER='5 s' bash scripts/flink-archive.sh     # 체크포인트(10초)보다 짧게 잘라 보기
#
# Parquet 은 bulk 포맷이라 롤링 간격과 상관없이 체크포인트마다 파일을 닫는다.
# 실제 롤링 = min(ROLLOVER, 체크포인트 간격 10초). 그래서 파일은 최대 약 10초 안에 보인다.
# ROLLOVER 를 10초보다 길게 잡아도 효과가 없다 (docs/08-minio-guide.md 4절 실측).
# ---------------------------------------------------------------------------
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

STARTUP="${STARTUP_MODE:-latest-offset}"
IDLE="${SOURCE_IDLE_TIMEOUT:-5}"
ROLLOVER="${ROLLOVER:-1 min}"

mkdir -p data/flink
sed -e "s/__STARTUP_MODE__/${STARTUP}/g" \
    -e "s/__IDLE_TIMEOUT__/${IDLE}/g" \
    -e "s/__ROLLOVER__/${ROLLOVER}/g" \
    sql/archive.sql > data/flink/archive.rendered.sql

echo "[flink-archive] startup=${STARTUP} rollover='${ROLLOVER}' idle=${IDLE}s"
docker compose exec -T jobmanager /opt/flink/bin/sql-client.sh -f /data/flink/archive.rendered.sql
rc=$?

echo
curl -fsS http://localhost:8181/jobs/overview 2>/dev/null | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
run = [j for j in d.get('jobs', []) if j.get('state') == 'RUNNING']
print('  실행 중인 잡 %d개 (로컬 슬롯 2개)' % len(run))
for j in run:
    print('    %-22s %s  tasks %s' % (j.get('name'), j.get('state'), j.get('tasks', {}).get('total')))
" 2>/dev/null
exit $rc
