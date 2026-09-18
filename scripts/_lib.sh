#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 시나리오 스크립트 공용 함수.
#
# 스냅샷은 대시보드 API(/api/overview)를 그대로 쓴다.
# 이미 여섯 군데를 합쳐 주는 곳이 있는데 스크립트가 또 긁을 이유가 없다.
# ---------------------------------------------------------------------------
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
export PYTHONIOENCODING=utf-8

DASH="${DASH:-http://localhost:8088}"
COLLECTOR="${COLLECTOR:-http://localhost:8080}"
FLINK="${FLINK:-http://localhost:8181}"

# MSYS_NO_PATHCONV 를 켜면 Windows curl.exe 가 /dev/null 을 리터럴 경로로 받는다.
DEVNULL=/dev/null
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DEVNULL=NUL;; esac
export DEVNULL

C_B=$'\033[1m'; C_G=$'\033[32m'; C_Y=$'\033[33m'; C_R=$'\033[31m'; C_D=$'\033[90m'; C_0=$'\033[0m'

banner() { printf '\n%s========================================================================%s\n' "$C_B" "$C_0"
           printf '%s  %s%s\n' "$C_B" "$1" "$C_0"
           printf '%s========================================================================%s\n' "$C_B" "$C_0"; }
step()   { printf '\n%s>> %s%s\n' "$C_G" "$1" "$C_0"; }
note()   { printf '%s   %s%s\n' "$C_D" "$1" "$C_0"; }
warn()   { printf '%s   %s%s\n' "$C_Y" "$1" "$C_0"; }
watch()  { printf '\n%s   [대시보드에서 볼 것] %s%s\n' "$C_Y" "$1" "$C_0"; }

# snap <라벨>  : 현재 상태 한 줄 요약
snap() {
  local label="$1"
  curl -fsS --max-time 12 "$DASH/api/overview" 2>/dev/null | python -c "
import sys, json
label = sys.argv[1]
try:
    d = json.load(sys.stdin)
except Exception:
    print('  %-14s (대시보드 API 응답 없음)' % label); sys.exit(0)
i = d.get('ingest', {}); f = d.get('flink', {}); o = d.get('outbox', {})
r = d.get('realtime', {}); k = d.get('kafka', {})
tp = {t['topic']: t['produced'] for t in k.get('topics', [])}
print('  %-14s 수신 %8d (%6.0f/s)  실패 %6d  fb %5d(대기%4d)  dlq %6d | '
      'flink in %8d dedup %6d late %6d | outbox 미발행 %4d(지연%3ds) | '
      'redis late %6d alert %4d | inst %s'
      % (label, i.get('received',0), i.get('received_rate',0), i.get('invalid',0),
         i.get('fallback',0), i.get('fallback_pending',0), i.get('dlq',0),
         f.get('input',0), f.get('dedup_removed',0), f.get('late_out',0),
         o.get('unpublished',0), o.get('lag_seconds',0),
         r.get('late_total',0), r.get('alerts_total',0), i.get('replicas','?')))
" "$label"
}

snap_header() {
  printf '%s  %-14s %s%s\n' "$C_D" "시점" \
    "수신 (초당)  검증실패  fallback  DLQ | Flink 입력/중복제거/late | Outbox | Redis late/alert | 인스턴스" "$C_0"
}

# 대시보드가 떠 있는지 확인
require_dash() {
  if ! curl -fsS --max-time 3 "$DASH/api/health" >/dev/null 2>&1; then
    printf '%s대시보드 API(%s)가 응답하지 않는다. 먼저 스택을 올릴 것: make up%s\n' "$C_R" "$DASH" "$C_0"
    exit 1
  fi
}

# Flink 잡이 떠 있는지 확인
require_flink_jobs() {
  local n
  n=$(curl -fsS --max-time 3 "$FLINK/jobs/overview" 2>/dev/null \
      | python -c "import sys,json;print(sum(1 for j in json.load(sys.stdin).get('jobs',[]) if j['state']=='RUNNING'))" 2>/dev/null || echo 0)
  if [[ "${n:-0}" -lt 1 ]]; then
    warn "실행 중인 Flink 잡이 없다. 먼저: bash scripts/flink-submit.sh (그리고 flink-archive.sh)"
  fi
}

countdown() {  # countdown <초> <메시지>
  local n="$1"
  # 파이프로 넘길 때(비 tty)는 \r 이 안 먹어 로그가 지저분해진다. 한 줄만 찍고 잔다.
  if [[ ! -t 1 ]]; then
    printf '%s   %s ... %ds%s\n' "$C_D" "$2" "$n" "$C_0"
    sleep "$n"; return
  fi
  while (( n > 0 )); do
    printf '\r%s   %s ... %2ds %s' "$C_D" "$2" "$n" "$C_0"
    sleep 1; n=$((n-1))
  done
  printf '\r%*s\r' 70 ''
}
