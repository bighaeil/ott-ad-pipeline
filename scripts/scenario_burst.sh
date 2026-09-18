#!/usr/bin/env bash
# ===========================================================================
# 시나리오 4 — 지연 폭주 (burst) -> late.events 증가와 실시간/확정 차이 확대
#
# 생성기의 burst 모드는 전송만 멈추고 생성은 계속한다.
# 정지 시간이 워터마크 허용치(기본 10초)보다 크면, 재개 순간 쏟아진 이벤트가
# 전부 "윈도우를 놓친 지각 이벤트" 가 된다.
# ===========================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
source scripts/_lib.sh

PAUSE="${BURST_PAUSE:-20}"      # 워터마크(10초)보다 크게 잡아야 지각이 확실히 생긴다
PERIOD="${BURST_PERIOD:-40}"
DURATION="${DURATION:-160}"

require_dash
require_flink_jobs

banner "시나리오 4: 지연 폭주 -> late.events 급증"
note "전송 정지 ${PAUSE}초 / 주기 ${PERIOD}초. 워터마크 허용치보다 정지가 길다."
watch "[처리] late.events 가 계단식으로 뛰는 것,  [버퍼] 토픽 막대가 0 이었다가 폭증하는 것"
snap_header
snap "시작"

step "1) 기준선 — 확정 집계를 먼저 만들어 둔다"
bash scripts/flush-windows.sh >/dev/null 2>&1
bash scripts/batch.sh >/dev/null 2>&1
BASE=$(bash scripts/recon.sh 2>&1 | grep -E "^  합계" | head -1)
echo "  기준선$BASE"

step "2) burst 부하 ${DURATION}초"
warn "생성기 콘솔의 buf 열을 볼 것. 0 -> 수천 -> 0 을 반복한다."
MODE=burst USERS=300 DURATION=$DURATION bash scripts/load.sh \
  --burst-pause "$PAUSE" --burst-period "$PERIOD" 2>&1 | tail -10
snap "burst후"

step "3) 윈도우 마감 + 재집계"
bash scripts/flush-windows.sh 2>&1 | tail -2
bash scripts/batch.sh 2>&1 | grep -E "총 행수|제거된 중복|채택|버린 이중경로" | sed 's/^/  /'

step "4) 대사 — 차이가 벌어졌는지"
AFTERL=$(bash scripts/recon.sh 2>&1 | tee data/recon_after.txt | grep -E "^  합계" | head -1)
sed -n "/실시간 vs 확정/,/^  해석/p" data/recon_after.txt
echo
echo "  ---- 기준선 대비 ----"
echo "  before$BASE"
echo "  after $AFTERL"
warn "차이율 자체는 크게 안 움직인다. 대사가 '하루 누적' 이라 burst 한 번은 희석되기 때문이다."
warn "burst 의 흔적은 아래 lateness 분포와 late.events 증가량에서 훨씬 선명하게 보인다."

step "5) 지각 이벤트 실물"
echo "  --- lateness_ms 분포 (상위 5) ---"
docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
    --bootstrap-server localhost:9092 --topic late.events \
    --from-beginning --timeout-ms 10000 2>/dev/null \
  | python -c "
import sys, json, collections
c = collections.Counter(); n = 0
for line in sys.stdin:
    try: d = json.loads(line)
    except Exception: continue
    n += 1
    c[(d.get('lateness_ms') or 0)//10000*10] += 1
print('    총 %d건' % n)
for k, v in sorted(c.items())[:8]:
    print('    %3d~%3d초 : %6d %s' % (k, k+10, v, '#'*min(v//20, 50)))
"

banner "정리"
note "정지 ${PAUSE}초 > 워터마크 10초 이므로, 재개 순간 쏟아진 이벤트는 이미 윈도우를 놓쳤다."
note "실시간 집계는 그 이벤트를 못 세고(late.events 로 빠짐), 배치는 event_time 만 보므로 센다."
note "-> 확정 > 실시간 방향으로 차이가 벌어진다. 대사 표의 '지연반영 +N' 항이 그것이다."
note "워터마크를 늘리면(FLINK_WATERMARK_DELAY=30 make flink) 지각이 줄지만 집계가 그만큼 늦어진다."
