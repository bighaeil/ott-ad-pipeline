#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 파이프라인 상태를 2초마다 갱신해서 보여준다.
# 단계 6의 대시보드가 생기기 전까지 쓰는 임시 관찰 창.
#
#   bash scripts/observe.sh            # 2초 간격
#   INTERVAL=1 bash scripts/observe.sh # 1초 간격
#
# 보여주는 것
#   토픽별 누적 메시지 수 + 직전 주기 대비 증가분(초당)
#   Collector 카운터 (수신 / 검증실패 / fallback / DLQ)
# ---------------------------------------------------------------------------
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

INTERVAL="${INTERVAL:-2}"
COLLECTOR="${COLLECTOR:-http://localhost:8080}"
ADDECISION="${ADDECISION:-http://localhost:8090}"
FLINK="${FLINK:-http://localhost:8181}"

declare -A PREV
FIRST=1

metric() {  # metric <prom_text> <metric_name>  -> 라벨 무시하고 전부 더한다
  echo "$1" | awk -v m="$2" '$1 ~ "^"m"[{ ]" { s += $NF } END { printf "%.0f", s+0 }'
}

cleanup() { printf '\033[?25h\n'; exit 0; }
trap cleanup INT TERM

printf '\033[?25l'   # 커서 숨김

while true; do
  OFFSETS=$(docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
              --bootstrap-server localhost:9092 2>/dev/null \
            | awk -F: '{s[$1]+=$3} END {for (t in s) print t, s[t]}' | sort)
  PROM=$(curl -s --max-time 3 "$COLLECTOR/actuator/prometheus" 2>/dev/null)
  APROM=$(curl -s --max-time 3 "$ADDECISION/actuator/prometheus" 2>/dev/null)

  OUT=""
  OUT+=$(printf '\033[1m%-18s %12s %10s\033[0m\n' "TOPIC" "누적" "건/초")
  OUT+=$'\n'
  while read -r topic count; do
    [[ -z "$topic" || "$topic" == __* ]] && continue
    prev="${PREV[$topic]:-$count}"
    rate=$(awk -v a="$count" -v b="$prev" -v i="$INTERVAL" 'BEGIN{printf "%.0f",(a-b)/i}')
    PREV[$topic]=$count
    mark=""
    [[ "$rate" -gt 0 ]] && mark="  <"
    OUT+=$(printf '%-18s %12s %10s%s\n' "$topic" "$count" "$rate" "$mark")
    OUT+=$'\n'
  done <<< "$OFFSETS"

  recv=$(metric "$PROM" collector_events_received_total)
  inval=$(metric "$PROM" collector_events_invalid_total)
  fb=$(metric "$PROM" collector_events_fallback_total)
  fbp=$(metric "$PROM" collector_fallback_pending)
  kerr=$(metric "$PROM" collector_kafka_publish_errors_total)

  prev_recv="${PREV[__recv]:-$recv}"
  recv_rate=$(awk -v a="$recv" -v b="$prev_recv" -v i="$INTERVAL" 'BEGIN{printf "%.0f",(a-b)/i}')
  PREV[__recv]=$recv

  clear
  echo "== OTT 광고 파이프라인 관찰  ($(date '+%H:%M:%S'), ${INTERVAL}초 간격, Ctrl+C 종료) =="
  echo
  echo "$OUT"
  echo "------------------------------------------------------------------"
  printf 'Collector  수신 %s (%s/s)   검증실패 %s   fallback %s (미재적재 %s)   kafka오류 %s\n' \
     "$recv" "$recv_rate" "$inval" "$fb" "$fbp" "$kerr"
  if [[ -z "$PROM" ]]; then
    echo "  (collector 응답 없음 - 컨테이너가 떠 있는지 확인)"
  fi

  ob_unpub=$(metric "$APROM" outbox_unpublished)
  ob_lag=$(metric "$APROM" outbox_lag_seconds)
  ob_pub=$(metric "$APROM" outbox_published_total)
  ob_dup=$(metric "$APROM" outbox_republished_total)
  ad_fill=$(echo "$APROM" | awk '/^addecision_requests_total\{.*fill="true"/ {s+=$NF} END{printf "%.0f", s+0}')
  ad_nofill=$(echo "$APROM" | awk '/^addecision_requests_total\{.*fill="false"/ {s+=$NF} END{printf "%.0f", s+0}')
  if [[ -n "$APROM" ]]; then
    printf 'Outbox     미발행 %s (최고지연 %ss)   발행 %s   재발행(중복) %s   |  광고요청 fill %s / nofill %s\n' \
       "$ob_unpub" "$ob_lag" "$ob_pub" "$ob_dup" "$ad_fill" "$ad_nofill"
  else
    echo "Outbox     (ad-decision 응답 없음)"
  fi

  # --- Flink ---
  FJ=$(curl -s --max-time 3 "$FLINK/jobs/overview" 2>/dev/null)
  if [[ -n "$FJ" ]]; then
    echo "$FJ" | python -c "
import sys, json
try:
    d = json.load(sys.stdin)
except Exception:
    sys.exit(0)
jobs = [j for j in d.get('jobs', []) if j.get('state') == 'RUNNING']
if not jobs:
    print('Flink      실행 중인 잡 없음  (bash scripts/flink-submit.sh)')
for j in jobs:
    t = j.get('tasks', {})
    print('Flink      %-18s %s  tasks %s/%s' % (j.get('name'), j.get('state'), t.get('running'), t.get('total')))
" 2>/dev/null
  else
    echo "Flink      (jobmanager 응답 없음)"
  fi

  # --- Redis 실시간 집계 ---
  RKEYS=$(docker compose exec -T redis redis-cli --scan --pattern 'agg:1m:*' 2>/dev/null | tr -d '
' | wc -l)
  RLATE=$(docker compose exec -T redis redis-cli get late:total 2>/dev/null | tr -d '
')
  RALERT=$(docker compose exec -T redis redis-cli get alerts:total 2>/dev/null | tr -d '
')
  RLAST=$(docker compose exec -T redis redis-cli zrevrange agg:index 0 0 2>/dev/null | tr -d '
')
  printf 'Redis      분단위 키 %s개   late 누계 %s   alert 누계 %s   최신 %s\n' \
     "${RKEYS:-0}" "${RLATE:-0}" "${RALERT:-0}" "${RLAST:-(없음)}"

  if [[ $FIRST -eq 1 ]]; then
    FIRST=0
    echo
    echo "첫 화면의 '건/초' 는 기준점이 없어 0 이다. 다음 갱신부터 의미가 있다."
  fi
  sleep "$INTERVAL"
done
