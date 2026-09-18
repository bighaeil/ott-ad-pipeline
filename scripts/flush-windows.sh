#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 열려 있는 Flink 윈도우를 닫는다.
#
# 왜 필요한가
#   Flink 이벤트타임 윈도우는 "워터마크가 윈도우 끝을 지날 때" 발화한다.
#   워터마크는 들어오는 이벤트의 event_time 에서 나온다.
#   그래서 부하를 멈추면 워터마크도 멈추고, 마지막 1~2분 윈도우가 영원히 안 닫힌다.
#   -> Redis 실시간 합계에 마지막 구간이 빠지고, 대사에서 큰 잔차로 나타난다.
#
#   운영에서는 트래픽이 끊기지 않으므로 저절로 해결된다.
#   로컬에서 "부하 -> 배치 -> 대사" 를 한 번에 돌릴 때만 필요한 장치다.
#
# 하는 일
#   검증용 캠페인(cmp-9999)으로 소량 이벤트를 몇 번 나눠 보내 워터마크를 앞으로 민다.
#   이 캠페인은 생성기가 쓰지 않으므로 다른 캠페인 숫자를 오염시키지 않고,
#   실시간/확정 양쪽에 똑같이 들어가므로 대사 결과도 왜곡하지 않는다.
# ---------------------------------------------------------------------------
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

# MSYS_NO_PATHCONV 를 켜면 Windows curl.exe 가 /dev/null 을 리터럴 경로로 받는다.
DEVNULL=/dev/null
case "$(uname -s)" in MINGW*|MSYS*|CYGWIN*) DEVNULL=NUL;; esac

COLLECTOR="${COLLECTOR:-http://localhost:8080}"
ROUNDS="${ROUNDS:-3}"
GAP="${GAP:-25}"

# 검증용 캠페인이 없으면 만든다 (정산 단가 0 -> 금액에 영향 없음)
docker compose exec -T postgres psql -U "${POSTGRES_USER:-ads}" -d "${POSTGRES_DB:-adplatform}" -q -c \
 "insert into campaigns(campaign_id,advertiser,name,vertical,daily_budget)
    values ('cmp-9999','테스트광고주','윈도우 플러시용','test',0) on conflict do nothing;
  insert into campaign_rates(campaign_id,cpm,cpc)
    values ('cmp-9999',0,0) on conflict do nothing;" >/dev/null 2>&1

echo "[flush] 워터마크 밀기: ${ROUNDS}회 x ${GAP}초"
for i in $(seq 1 "$ROUNDS"); do
  TS=$(date +%s%3N)
  R="wmflush-$TS"
  curl -sS -o "$DEVNULL" -X POST "$COLLECTOR/v1/events" -H 'Content-Type: application/json' \
    --data-binary "[
      {\"event_id\":\"$R-i\",\"event_type\":\"impression\",\"campaign_id\":\"cmp-9999\",\"event_time\":$TS,\"ad_request_id\":\"$R\"},
      {\"event_id\":\"$R-q\",\"event_type\":\"quartile\",\"quartile\":\"complete\",\"campaign_id\":\"cmp-9999\",\"event_time\":$TS,\"ad_request_id\":\"$R\"},
      {\"event_id\":\"$R-c\",\"event_type\":\"click\",\"campaign_id\":\"cmp-9999\",\"event_time\":$TS,\"ad_request_id\":\"$R\"},
      {\"event_id\":\"$R-r\",\"event_type\":\"ad_response\",\"fill\":true,\"campaign_id\":\"cmp-9999\",\"event_time\":$TS,\"ad_request_id\":\"$R\",\"source\":\"server\"}
    ]"
  echo "  $i/$ROUNDS 전송 (t=$(date -u '+%H:%M:%S'))"
  if [[ "$i" -lt "$ROUNDS" ]]; then sleep "$GAP"; fi
done

echo "[flush] 윈도우 발화 + Parquet 커밋 대기 40초"
sleep 40
echo "[flush] 완료. 이제 make batch -> make recon"
