#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 원본 Parquet 적재 잡 제출 (MinIO).
#
#   bash scripts/flink-archive.sh
#   ROLLOVER='20 s' bash scripts/flink-archive.sh    # 파일이 빨리 보이게
#
# 파일이 MinIO 에 나타나기까지 걸리는 시간 = 롤링 간격 + 다음 체크포인트(10초).
# 기본 롤링 1분이면 최대 70초쯤 걸린다. 급하면 ROLLOVER 를 줄인다.
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
