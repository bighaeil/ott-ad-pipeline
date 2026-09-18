#!/usr/bin/env bash
# ===========================================================================
# 시나리오 2 — Kafka 정지 -> Collector fail-open -> 재기동 후 재적재
#
# 두 가지 대응을 나란히 본다.
#   Collector  : 로컬 파일에 흘리고 200 을 준다. 사람이 재적재를 호출해야 한다.
#   ad-decision: DB 에 이미 있으므로 Outbox 워커가 알아서 따라잡는다.
# ===========================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
source scripts/_lib.sh

DOWN_S="${DOWN_S:-45}"
LOAD_S="${LOAD_S:-150}"

require_dash

banner "시나리오 2: Kafka 정지 -> fail-open -> 재적재"
note "부하를 흘리는 도중에 브로커를 죽인다. 수집은 계속 200 을 준다."
watch "[수집] fallback 과 '미재적재' 가 오르는 것,  [버퍼] 모든 토픽 막대가 0 으로 죽는 것,  [정합성] Outbox 미발행/최고지연 상승"
snap_header
snap "시작"

step "1) 부하 ${LOAD_S}초를 백그라운드로 시작"
( MODE=normal USERS=300 DURATION=$LOAD_S bash scripts/load.sh >/dev/null 2>&1 & )
countdown 20 "부하 안정화 대기"
snap "부하중"

step "2) Kafka 정지"
docker compose stop kafka >/dev/null 2>&1
warn "이 순간부터 Collector 는 Kafka 대신 /data/fallback 에 쓴다."
for i in 1 2 3; do
  countdown $((DOWN_S / 3)) "정지 상태 유지"
  snap "정지+$((i * DOWN_S / 3))s"
done

step "3) 정지 중에 무슨 일이 있었나"
echo "  --- fallback 파일 ---"
ls -l data/fallback/*.jsonl 2>/dev/null | sed 's/^/    /' || echo "    (없음)"
echo "  --- fallback 상태 API ---"
curl -fsS "$COLLECTOR/v1/admin/fallback" 2>/dev/null | sed 's/^/    /'; echo
echo "  --- Collector 로그의 FAIL-OPEN ---"
docker compose logs collector --tail=400 2>&1 | grep -i "FAIL-OPEN" | tail -3 | sed 's/^/    /'
echo "  --- Outbox 적체 ---"
curl -fsS "http://localhost:8090/v1/outbox/stats" 2>/dev/null | sed 's/^/    /'; echo

step "4) Kafka 재기동"
docker compose start kafka >/dev/null 2>&1
for i in $(seq 1 40); do
  docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh \
      --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 2
done
note "브로커 정상"
countdown 15 "Outbox 워커 자동 배수 대기"
snap "재기동후"
echo "  --- Outbox (사람 개입 없이 스스로 따라잡았는지) ---"
curl -fsS "http://localhost:8090/v1/outbox/stats" 2>/dev/null | sed 's/^/    /'; echo

step "5) Collector fallback 재적재 (이쪽은 사람이 호출해야 한다)"
curl -fsS -X POST "$COLLECTOR/v1/admin/fallback/replay" 2>/dev/null | sed 's/^/    /'; echo
sleep 3
echo "  --- 파일 상태 (.done 으로 rename 됐는지) ---"
ls -l data/fallback/ 2>/dev/null | tail -5 | sed 's/^/    /'
snap "재적재후"

banner "정리"
note "Collector : 유실 0. 다만 응답이 약 3초로 늘어났고(=max.block.ms + Reactor timeout),"
note "            복구는 사람이 /v1/admin/fallback/replay 를 눌러야 한다."
note "ad-decision: DB 에 남아 있어 워커가 스스로 따라잡았다. 대신 재발행 중복이 생긴다."
note "재적재분은 event_id 가 같으므로 Flink(1시간 TTL)와 Spark(전체 범위)가 모두 걷어낸다."
echo
note "확인: bash scripts/flush-windows.sh && bash scripts/batch.sh   (중복 제거 건수가 늘어난다)"
