#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# Flink SQL 파이프라인 제출.
#
#   bash scripts/flink-submit.sh
#   FLINK_WATERMARK_DELAY=2 bash scripts/flink-submit.sh    # 지각 이벤트를 늘려 본다
#   STARTUP_MODE=earliest-offset bash scripts/flink-submit.sh
#
# SQL Client 는 변수 치환을 못 하므로 자리표시자를 sed 로 바꾼 뒤 넘긴다.
# 치환 결과는 data/flink/pipeline.rendered.sql 에 남으니 실제로 뭐가 실행됐는지 볼 수 있다.
# ---------------------------------------------------------------------------
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

WM="${FLINK_WATERMARK_DELAY:-10}"          # 워터마크 허용 지연(초)
STARTUP="${STARTUP_MODE:-latest-offset}"   # latest-offset | earliest-offset
THRESHOLD="${ALERT_THRESHOLD:-0.5}"        # impression/request 경고 임계치
MIN_REQ="${ALERT_MIN_REQUESTS:-5}"         # 표본이 너무 적으면 경고하지 않는다
IDLE="${SOURCE_IDLE_TIMEOUT:-5}"           # 유휴 파티션을 워터마크 계산에서 빼기까지의 시간(초)
CHAINING="${FLINK_OPERATOR_CHAINING:-false}"   # false 면 연산자별 계수가 REST 로 보인다

mkdir -p data/flink
sed -e "s/__WATERMARK_DELAY__/${WM}/g" \
    -e "s/__STARTUP_MODE__/${STARTUP}/g" \
    -e "s/__ALERT_THRESHOLD__/${THRESHOLD}/g" \
    -e "s/__ALERT_MIN_REQUESTS__/${MIN_REQ}/g" \
    -e "s/__IDLE_TIMEOUT__/${IDLE}/g" \
    -e "s/__CHAINING__/${CHAINING}/g" \
    sql/pipeline.sql > data/flink/pipeline.rendered.sql

echo "[flink-submit] watermark=${WM}s idle=${IDLE}s startup=${STARTUP} threshold=${THRESHOLD} min_requests=${MIN_REQ}"
echo "[flink-submit] 렌더된 SQL: data/flink/pipeline.rendered.sql"

# 대사용 테이블(late_dropped). 새 볼륨이면 init 스크립트가 만들지만, 기존 볼륨에는 없으므로 여기서 맞춘다.
if ! docker compose exec -T postgres psql -U ads -d adplatform -q -v ON_ERROR_STOP=1      < postgres/init/02-late-dropped.sql >/dev/null; then
  echo "[flink-submit] late_dropped 테이블 준비 실패 - postgres 가 떠 있는지 확인할 것."
  exit 1
fi

# 이미 같은 이름의 잡이 돌고 있으면 먼저 알려 준다.
RUNNING=$(curl -fsS http://localhost:8181/jobs/overview 2>/dev/null \
          | tr ',' '\n' | grep -c '"state":"RUNNING"' || true)
if [[ "${RUNNING:-0}" -gt 0 ]]; then
  echo "[flink-submit] 경고: 이미 RUNNING 잡이 ${RUNNING}개 있다. 슬롯은 2개뿐이니 필요하면 먼저 취소할 것."
  echo "               bash scripts/flink-cancel.sh"
fi

# SQL Client 는 문장이 실패해도 종료 코드 0 을 돌려준다. 출력의 [ERROR] 로 판정한다.
LOG=data/flink/pipeline.submit.log
docker compose exec -T jobmanager /opt/flink/bin/sql-client.sh -f /data/flink/pipeline.rendered.sql 2>&1 | tee "$LOG"
rc=${PIPESTATUS[0]}
if [[ $rc -eq 0 ]] && grep -q "\[ERROR\]" "$LOG"; then
  rc=1
fi

echo
if [[ $rc -eq 0 ]]; then
  echo "[flink-submit] 제출 완료. Flink UI: http://localhost:8181"
  curl -fsS http://localhost:8181/jobs/overview 2>/dev/null \
    | python -c "
import sys,json
try:
    d=json.load(sys.stdin)
    for j in d.get('jobs',[]):
        print('  %-38s %-10s parallelism=%s' % (j.get('name'), j.get('state'), j.get('tasks',{}).get('total')))
except Exception:
    pass" 2>/dev/null
else
  echo "[flink-submit] 실패 (exit $rc). 위 오류 메시지를 확인할 것."
fi
exit $rc
