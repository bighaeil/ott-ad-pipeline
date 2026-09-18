#!/usr/bin/env bash
# ===========================================================================
# 시나리오 5 — 서명 위조 트래킹 호출 급증 -> DLQ 증가 + alert 발생
#
# VAST 트래킹 픽셀은 공개 URL 이라 누구나 호출할 수 있다.
# HMAC 서명이 없으면 임프레션 수를 마음대로 부풀릴 수 있다는 뜻이다.
# 여기서는 서명이 틀린 픽셀을 대량으로 쏴서 두 가지를 확인한다.
#   1) 위조분이 집계에 섞이지 않고 dlq.invalid 로 격리되는가
#   2) 정상 임프레션이 사라져 impression/request 비율이 무너지고 alert 가 뜨는가
# ===========================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
source scripts/_lib.sh

DURATION="${DURATION:-120}"
FORGE="${FORGE:-0.9}"

require_dash
require_flink_jobs

banner "시나리오 5: 서명 위조 급증 -> DLQ + alert"
note "전체 이벤트의 ${FORGE} 비율을 서명이 틀린 픽셀로 보낸다."
watch "[수집] DLQ 누적과 track_bad_signature 카운터,  [경고] alert.anomaly 목록,  [정합성] imp/req 비율 붕괴"
snap_header
snap "시작"

step "1) 평시 30초 (비교 기준)"
MODE=normal USERS=250 DURATION=30 bash scripts/load.sh 2>&1 | grep -E "서명위조|HTTP" | sed 's/^/  /'
snap "평시"
echo "  --- 평시 DLQ 사유 분포 ---"
curl -fsS "$COLLECTOR/actuator/prometheus" 2>/dev/null \
  | grep '^collector_dlq_total' | sed 's/application="collector",//' | sed 's/^/    /'

step "2) 위조 급증 ${DURATION}초 (forge=${FORGE}, 픽셀 비율 0.9)"
warn "정상 임프레션이 거의 사라지므로 imp/req 비율이 임계치 0.5 밑으로 내려간다."
MODE=normal USERS=300 DURATION=$DURATION bash scripts/load.sh \
  --forge "$FORGE" --pixel-ratio 0.9 2>&1 | tail -8
snap "위조중"

step "3) DLQ 확인"
echo "  --- DLQ 사유 분포 ---"
curl -fsS "$COLLECTOR/actuator/prometheus" 2>/dev/null \
  | grep '^collector_dlq_total' | sed 's/application="collector",//' | sed 's/^/    /'
echo "  --- dlq.invalid 실물 (위조분 1건) ---"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic dlq.invalid \
    --from-beginning --timeout-ms 8000 2>/dev/null \
  | grep track_bad_signature | tail -1 | cut -c1-400 | sed 's/^/    /'

step "4) 윈도우 마감 후 alert 확인"
bash scripts/flush-windows.sh >/dev/null 2>&1
echo "  --- 최근 alert.anomaly ---"
curl -fsS "http://localhost:8099/alerts?limit=8" 2>/dev/null | python -c "
import sys, json
try: a = json.load(sys.stdin).get('alerts', [])
except Exception: a = []
if not a: print('    (없음)')
for x in a:
    print('    %s %-9s imp=%-5s req=%-5s ratio=%.3f (임계 %s)'
          % (x['window_start'][11:16], x['campaign_id'], x['impressions'],
             x['requests'], x.get('imp_req_ratio') or 0, x['threshold']))
"
snap "종료"

banner "정리"
note "위조 픽셀은 200 + 1x1 GIF 로 응답한다. 4xx 를 주면 플레이어가 재시도 폭주를 일으키기 때문이다."
note "대신 dlq.invalid 에 reason=track_bad_signature 와 원본 쿼리스트링이 통째로 남는다."
note "집계에는 한 건도 섞이지 않는다 -> 서명 검증이 정산 방어선이라는 뜻이다."
note "부수 효과: 정상 임프레션이 줄어 imp/req 비율이 무너지고 alert 가 뜬다."
note "실제 상황이라면 이 alert 가 '광고 사기' 가 아니라 '플레이어 장애' 로 오인될 수 있다."
note "그래서 DLQ 사유별 카운터를 alert 와 같이 봐야 한다."
