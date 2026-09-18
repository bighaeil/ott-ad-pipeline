#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# 토픽 생성. 멱등(이미 있으면 건너뜀).
#
# 파티션 수: 로컬 6 / 실제 운영 48
#   - 파티션 키는 ad_request_id 이므로, 하나의 광고 요청에서 파생된
#     request/impression/quartile/click 이벤트가 같은 파티션에 모인다.
#     Flink 에서 ad_request_id 단위 상태 처리를 로컬리티 있게 하기 위한 선택.
#   - 운영 48은 피크 2,000 EPS x 여유배수 기준. 로컬은 슬롯 2개뿐이라 6으로 축소.
# 복제 계수: 로컬 1 / 운영 3
# ---------------------------------------------------------------------------
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-kafka:9092}"
PARTITIONS="${PARTITIONS:-6}"          # 운영: 48
REPLICATION="${REPLICATION:-1}"        # 운영: 3
RETENTION_MS="${RETENTION_MS:-86400000}"
KT=/opt/kafka/bin/kafka-topics.sh

# 데이터 토픽
TOPICS=(
  ad.impression
  ad.quartile
  ad.click
  ad.request
  user.behavior
  dlq.invalid
)

# 단계 4 이후에 쓰는 파생 토픽. 미리 만들어 둔다.
#   late.events   : 워터마크를 지나 도착해 윈도우를 놓친 이벤트
#   alert.anomaly : impression/request 비율 이상 경고
#   agg.minute    : Flink 의 분단위 집계 결과. redis-writer 가 읽어 Redis 에 넣는다.
#                   (Flink 1.20 용 Redis SQL 커넥터가 없어 생긴 우회 경로)
DERIVED=(
  late.events
  alert.anomaly
  agg.minute
)

create() {
  local name="$1" parts="$2"
  if $KT --bootstrap-server "$BOOTSTRAP" --list | grep -qx "$name"; then
    echo "[kafka-init] exists  : $name"
  else
    $KT --bootstrap-server "$BOOTSTRAP" --create \
        --topic "$name" \
        --partitions "$parts" \
        --replication-factor "$REPLICATION" \
        --config retention.ms="$RETENTION_MS" \
        --config cleanup.policy=delete
    echo "[kafka-init] created : $name (partitions=$parts rf=$REPLICATION)"
  fi
}

echo "[kafka-init] bootstrap=$BOOTSTRAP partitions=$PARTITIONS(운영 48) rf=$REPLICATION(운영 3)"

for t in "${TOPICS[@]}"; do
  create "$t" "$PARTITIONS"
done

# 파생 토픽은 트래픽이 훨씬 적으므로 파티션을 줄인다. (운영도 동일 정책: 데이터 토픽의 1/4 수준)
for t in "${DERIVED[@]}"; do
  create "$t" 2
done

echo "[kafka-init] --- final topic list ---"
$KT --bootstrap-server "$BOOTSTRAP" --list
echo "[kafka-init] done"
