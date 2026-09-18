#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 단계 1 확인용.
#   1) POST /v1/events  : 정상 2건 + 필수필드 누락 1건
#   2) GET  /v1/track   : 서명 정상 1건 + 서명 위조 1건
#   3) Kafka 콘솔 컨슈머로 각 토픽에 실제로 들어갔는지 확인
#   4) /actuator/prometheus 카운터 확인
# ---------------------------------------------------------------------------
set -uo pipefail
# Git Bash(MSYS) 가 /opt/... 같은 인자를 Windows 경로로 바꿔버리는 것을 막는다.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

# MSYS_NO_PATHCONV=1 을 켜면 Windows 용 curl.exe 가 "/dev/null" 을 리터럴 경로로
# 받아 "client returned ERROR on write" (curl 23) 를 낸다. OS 별로 갈라 준다.
DEVNULL=/dev/null
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DEVNULL=NUL;; esac

COLLECTOR="${COLLECTOR:-http://localhost:8080}"
SECRET="${COLLECTOR_HMAC_SECRET:-local-dev-secret}"
DC="docker compose"

NOW_MS=$(date +%s%3N)
NOW_S=$(date +%s)
EXP=$((NOW_S + 300))
RUN=$RANDOM$RANDOM

hr() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# ------------------------------------------------------- 1) POST /v1/events
hr "1) POST /v1/events  (정상 2건 + 스키마 위반 1건)"
ARID="req-smoke-$RUN"
read -r -d '' BODY <<JSON
[
  {
    "event_id": "evt-smoke-$RUN-1",
    "event_type": "impression",
    "campaign_id": "cmp-1001",
    "event_time": $NOW_MS,
    "ad_request_id": "$ARID",
    "creative_id": "crt-9001",
    "session_id": "sess-smoke-$RUN",
    "source": "client",
    "device": "smart_tv"
  },
  {
    "event_id": "evt-smoke-$RUN-2",
    "event_type": "quartile",
    "campaign_id": "cmp-1001",
    "event_time": $NOW_MS,
    "ad_request_id": "$ARID",
    "quartile": "complete",
    "session_id": "sess-smoke-$RUN"
  },
  {
    "event_id": "evt-smoke-$RUN-3",
    "event_type": "impression",
    "event_time": $NOW_MS,
    "ad_request_id": "$ARID"
  }
]
JSON

curl -sS -X POST "$COLLECTOR/v1/events" \
  -H 'Content-Type: application/json' \
  --data-binary "$BODY" -w '\nHTTP %{http_code}\n'
echo "  기대: accepted=2 invalid=1 (campaign_id 누락 1건이 dlq.invalid 로)"

# -------------------------------------------------------- 2) GET /v1/track
hr "2) GET /v1/track  (서명 정상 1건)"
EID="evt-smoke-$RUN-px"
# canonical = sig 를 제외한 파라미터를 key 오름차순으로 k=v 연결 (& 구분)
CANON="arid=$ARID&cid=cmp-1001&eid=$EID&et=impression&exp=$EXP&sid=sess-smoke-$RUN&src=server&ts=$NOW_MS"
SIG=$(printf '%s' "$CANON" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')
echo "  canonical: $CANON"
RESP=$(curl -sS -o "$DEVNULL" -D - "$COLLECTOR/v1/track?eid=$EID&et=impression&cid=cmp-1001&ts=$NOW_MS&arid=$ARID&sid=sess-smoke-$RUN&src=server&exp=$EXP&sig=$SIG")
printf '%s\n' "$RESP" | grep -Ei '^(HTTP|content-type)' | sed 's/^/  /'
echo "  기대: HTTP 200 + image/gif, ad.impression 토픽에 source=server 로 적재"

hr "2b) GET /v1/track  (서명 위조 1건)"
CODE=$(curl -sS -o "$DEVNULL" -w '%{http_code}' "$COLLECTOR/v1/track?eid=$EID-bad&et=impression&cid=cmp-1001&ts=$NOW_MS&arid=$ARID&sid=x&src=client&exp=$EXP&sig=deadbeef")
echo "  HTTP $CODE   (픽셀은 검증 실패해도 항상 200 이어야 정상)"
echo "  기대: dlq.invalid 에 reason=track_bad_signature"

sleep 2

# ------------------------------------------------- 3) Kafka 콘솔 컨슈머 확인
hr "3) Kafka 토픽 확인"
for T in ad.impression ad.quartile dlq.invalid; do
  echo "--- $T (이번 실행분만) ---"
  $DC exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh \
      --bootstrap-server localhost:9092 \
      --topic "$T" --from-beginning --timeout-ms 6000 2>/dev/null \
    | grep "$RUN" | sed 's/^/  /' || echo "  (없음)"
done

# ------------------------------------------------------------ 4) 메트릭 확인
hr "4) Prometheus 카운터 (/actuator/prometheus)"
curl -sS "$COLLECTOR/actuator/prometheus" \
  | grep -E '^collector_(events|dlq|kafka|fallback)' \
  | sed 's/^/  /'

hr "완료"
