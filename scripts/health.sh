#!/usr/bin/env bash
# 각 컴포넌트가 실제로 응답하는지 확인한다. compose 의 healthcheck 와 별개로
# "밖에서 보이는 상태"를 찍는 용도.
set -uo pipefail
# Git Bash(MSYS) 가 /opt/... 같은 인자를 Windows 경로로 바꿔버리는 것을 막는다.
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

ok()   { printf "  \033[32mOK\033[0m    %s\n" "$1"; }
bad()  { printf "  \033[31mFAIL\033[0m  %s\n" "$1"; }
wait_for() {  # wait_for <label> <max_seconds> <command...>
  local label="$1" max="$2"; shift 2
  local i=0
  while (( i < max )); do
    if "$@" >/dev/null 2>&1; then ok "$label"; return 0; fi
    sleep 2; i=$((i+2))
  done
  bad "$label (${max}s 초과)"; return 1
}

echo "== 헬스 체크 =="
rc=0
wait_for "kafka       (broker api)"   90 docker compose exec -T kafka /opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 || rc=1
wait_for "postgres    (pg_isready)"   60 docker compose exec -T postgres pg_isready -U "${POSTGRES_USER:-ads}" || rc=1
wait_for "redis       (PING)"         30 docker compose exec -T redis redis-cli ping || rc=1
wait_for "minio       (live)"         60 curl -fsS http://localhost:9000/minio/health/live || rc=1
wait_for "flink jm    (REST /overview)" 90 curl -fsS http://localhost:8181/overview || rc=1
wait_for "collector   (/actuator/health)" 120 bash -c 'curl -fsS http://localhost:8080/actuator/health | grep -q UP' || rc=1
wait_for "ad-decision (/actuator/health)" 120 bash -c 'curl -fsS http://localhost:8090/actuator/health | grep -q UP' || rc=1
wait_for "redis-writer(/)"            60 bash -c 'curl -fsS http://localhost:8099/ | grep -q UP' || rc=1
wait_for "dashboard   (/api/health)"  90 bash -c 'curl -fsS http://localhost:8088/api/health | grep -q UP' || rc=1
wait_for "tracer      (/api/health)"  90 bash -c 'curl -fsS http://localhost:3000/api/health | grep -q UP' || rc=1
wait_for "player      (/api/health)"  90 bash -c 'curl -fsS http://localhost:3001/api/health | grep -q UP' || rc=1

echo
echo "== 토픽 =="
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null | sed 's/^/  /'

echo
echo "== Outbox =="
curl -fsS http://localhost:8090/v1/outbox/stats 2>/dev/null | sed 's/^/  /' || echo "  (ad-decision 응답 없음)"
echo

echo "== Flink 슬롯 =="
curl -fsS http://localhost:8181/overview 2>/dev/null \
  | tr ',' '\n' | grep -E 'slots|taskmanagers' | sed 's/^/  /'

echo
if [[ $rc -eq 0 ]]; then
  echo "전부 정상."
  echo "  대시보드   : http://localhost:8088"
  echo "  이벤트추적 : http://localhost:3000"
  echo "  플레이어   : http://localhost:3001"
  echo "  다음     : make smoke -> make flink -> make archive -> make load"
else
  echo "일부 실패. docker compose logs -f <서비스> 로 확인."
fi
exit $rc
