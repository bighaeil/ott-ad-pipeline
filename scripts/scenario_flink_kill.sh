#!/usr/bin/env bash
# ===========================================================================
# 시나리오 3 — TaskManager 강제 종료 -> 체크포인트 복구 -> 집계 연속성 확인
#
# 확인하려는 것: 죽기 직전 윈도우의 집계값이 복구 후에도 빠짐없이 나오는가.
# Redis 의 분단위 키 집합을 죽이기 전/후로 비교해 "구멍 난 분" 이 있는지 본다.
# ===========================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
source scripts/_lib.sh

LOAD_S="${LOAD_S:-200}"

require_dash
require_flink_jobs

minutes_present() {   # Redis 에 있는 분단위 키의 분 목록
  docker compose exec -T redis redis-cli --scan --pattern 'agg:1m:*' 2>/dev/null \
    | tr -d '\r' | awk -F: '{print $NF}' | sort -u
}

cp_state() {
  curl -fsS "$FLINK/jobs/overview" 2>/dev/null | python -c "
import sys, json, urllib.request
try:
    d = json.load(sys.stdin)
except Exception:
    print('    (Flink 응답 없음)'); sys.exit(0)
for j in d.get('jobs', []):
    line = '    %-20s %-12s tasks %s/%s' % (j['name'], j['state'],
              j['tasks']['running'], j['tasks']['total'])
    try:
        cp = json.load(urllib.request.urlopen('$FLINK/jobs/%s/checkpoints' % j['jid'], timeout=3))
        c = cp['counts']
        latest = (cp.get('latest') or {}).get('completed') or {}
        line += '  체크포인트 완료 %s 실패 %s (최근 id=%s)' % (c['completed'], c['failed'], latest.get('id'))
    except Exception:
        pass
    print(line)
"
}

banner "시나리오 3: TaskManager kill -> 체크포인트 복구"
watch "[처리] 체크포인트 카운터와 tasks 수,  Flink UI(http://localhost:8181) 의 잡 상태 전이"
snap_header
snap "시작"

START_MIN=$(date -u +%Y%m%d%H%M)
note "시나리오 시작 분: $START_MIN"

step "1) 부하 ${LOAD_S}초를 백그라운드로 시작"
( MODE=normal USERS=300 DURATION=$LOAD_S bash scripts/load.sh >/dev/null 2>&1 & )
countdown 90 "윈도우가 몇 개 닫힐 때까지 대기"
echo "  --- 죽이기 전 Flink 상태 ---"; cp_state
BEFORE=$(minutes_present)
echo "  --- 죽이기 전 Redis 분단위 키 (마지막 5개) ---"
echo "$BEFORE" | tail -5 | sed 's/^/    /'
snap "kill직전"

KILL_MIN=$(date -u +%Y%m%d%H%M)

step "2) TaskManager 강제 종료 (SIGKILL)  [kill 시각 $KILL_MIN]"
docker compose kill taskmanager >/dev/null 2>&1
warn "JobManager 는 하트비트 타임아웃(로컬 15초 / 운영 기본 50초)이 지나야 사망을 인지한다."
warn "그전까지는 REST 가 여전히 RUNNING 을 보고한다. 이것도 관찰 포인트다."
sleep 6
echo "  --- kill 직후 6초 (아직 RUNNING 으로 보인다) ---"; cp_state
countdown 22 "하트비트 타임아웃 대기"
echo "  --- 타임아웃 후 (상태가 바뀐다) ---"; cp_state

step "3) TaskManager 재기동"
docker compose start taskmanager >/dev/null 2>&1
note "JobManager 의 재시작 전략이 잡을 마지막 체크포인트에서 되살린다."
# state 만 보면 안 된다. 슬롯이 없어도 잡은 RUNNING 으로 남고 tasks 만 0 이 된다.
# 모든 태스크가 실제로 배치될 때까지 기다린다.
for i in $(seq 1 60); do
  n=$(curl -fsS "$FLINK/jobs/overview" 2>/dev/null | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    print(0); raise SystemExit
print(sum(1 for j in d.get('jobs', [])
          if j['state'] == 'RUNNING' and j['tasks']['running'] == j['tasks']['total']))" 2>/dev/null || echo 0)
  [[ "${n:-0}" -ge 2 ]] && break
  sleep 3
done
note "모든 태스크 재배치까지 약 $((i * 3))초"
echo "  --- 복구 후 ---"; cp_state
snap "복구후"

step "4) 부하가 끝날 때까지 두고, 윈도우를 닫는다"
countdown 60 "잔여 부하 소진 대기"
bash scripts/flush-windows.sh 2>&1 | tail -2

step "5) 집계 연속성 검사 — 분단위 키에 구멍이 있는가"
AFTER=$(minutes_present)
export CHECK_FROM="$KILL_MIN"
echo "$AFTER" | python -c "
import sys
from datetime import datetime, timedelta
import os
start = os.environ.get('CHECK_FROM', '')
mins = sorted(m for m in set(x.strip() for x in sys.stdin if x.strip()) if m >= start)
if not mins:
    print('    (분단위 키 없음)'); sys.exit(0)
ts = [datetime.strptime(m, '%Y%m%d%H%M') for m in mins]
print('    검사 구간: kill 시각(%s) 이후' % start)
print('    범위: %s ~ %s  (총 %d분)' % (mins[0], mins[-1], len(mins)))
gaps = []
cur = ts[0]
while cur <= ts[-1]:
    if cur not in ts:
        gaps.append(cur.strftime('%H:%M'))
    cur += timedelta(minutes=1)
if gaps:
    print('    \033[33m빠진 분: %s\033[0m' % ', '.join(gaps))
    print('    (kill 이후 구간이다. 여기 구멍이 있으면 복구가 이벤트를 잃었다는 뜻이다.)')
else:
    print('    \033[32m구멍 없음 — kill 이후 모든 분이 빠짐없이 집계됐다\033[0m')
"

banner "정리"
note "TaskManager 를 SIGKILL 로 죽여도 집계가 이어지는 이유:"
note "  10초마다 /data/checkpoints 에 상태를 남기고, TM 이 돌아오면 그 지점부터 재개한다."
note "  Kafka 오프셋도 체크포인트에 함께 들어 있어 죽은 구간의 이벤트를 다시 읽는다."
note "  단, 재처리 구간은 at-least-once 라 중복이 생길 수 있다 -> Spark 배치가 정리한다."
echo
note "확인: bash scripts/batch.sh && bash scripts/recon.sh"
