#!/usr/bin/env bash
# ===========================================================================
# 시나리오 1 — 라이브 피크 (normal 200 EPS -> live 2000 EPS)
#
# Kubernetes HPA 가 없으므로 Collector 를 수동으로 스케일 아웃한다.
#   docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --no-deps --scale collector=N collector
#
# 기본 compose 는 8080 고정이라 --scale 이 포트 충돌로 실패한다.
# docker-compose.scale.yml 오버레이가 그때만 포트를 범위(8080-8085)로 바꾼다.
# collector 에 container_name 을 두지 않은 것도 --scale 때문이다.
# 컨테이너끼리는 Docker DNS 라운드로빈으로 collector:8080 을 나눠 쓴다.
#
# 관찰 포인트는 README "단계 7" 참조.
# ===========================================================================
set -uo pipefail
cd "$(dirname "$0")/.."
source scripts/_lib.sh

REPLICAS="${REPLICAS:-3}"
NORMAL_S="${NORMAL_S:-40}"
LIVE_S="${LIVE_S:-90}"

require_dash
require_flink_jobs

banner "시나리오 1: 라이브 피크 + Collector 수동 스케일 아웃"
note "평시(200 EPS) -> 인스턴스 1개→${REPLICAS}개 -> 피크(2000 EPS) -> 다시 1개"
watch "[수집] 초당 수신 / 인스턴스 수,  [버퍼] 토픽 막대와 consumer lag,  [처리] Flink 입력 건/초"
snap_header
snap "시작"

step "1) 평시 부하 ${NORMAL_S}초 (Collector 1대, 목표 200 EPS)"
MODE=normal USERS=300 DURATION=$NORMAL_S bash scripts/load.sh 2>&1 | tail -6
snap "평시종료"

step "2) Collector 를 ${REPLICAS}대로 스케일 아웃 (HPA 대용)"
docker compose -f docker-compose.yml -f docker-compose.scale.yml up -d --no-deps --scale collector=$REPLICAS collector 2>&1 | tail -4
note "기동 대기..."
for i in $(seq 1 30); do
  n=$(docker compose ps --format '{{.Service}}' 2>/dev/null | grep -c '^collector$')
  up=$(curl -fsS --max-time 3 "$DASH/api/overview" 2>/dev/null \
       | python -c "import sys,json;print(json.load(sys.stdin).get('ingest',{}).get('replicas_up',0))" 2>/dev/null || echo 0)
  [[ "${up:-0}" -ge "$REPLICAS" ]] && break
  sleep 3
done
docker compose ps --format 'table {{.Name}}\t{{.Service}}\t{{.Status}}' | grep -E 'collector|SERVICE'
snap "스케일후"

step "3) 라이브 피크 ${LIVE_S}초 (목표 2000 EPS)"
warn "생성기 콘솔의 p95 와 err 열을 같이 볼 것. 1대일 때와 비교하는 값이다."
MODE=live USERS=800 DURATION=$LIVE_S bash scripts/load.sh 2>&1 | tail -8
snap "피크종료"

step "4) 원래대로 축소 (1대, 포트도 8080 고정으로 복귀)"
docker compose -f docker-compose.yml -f docker-compose.scale.yml   up -d --no-deps --scale collector=1 collector >/dev/null 2>&1
docker compose up -d --no-deps --force-recreate collector 2>&1 | tail -3
for i in $(seq 1 20); do
  curl -fsS --max-time 3 "$COLLECTOR/actuator/health" 2>/dev/null | grep -q UP && break
  sleep 2
done
note "$COLLECTOR 복귀 확인"
snap "축소후"

banner "정리"
note "축소하면 Collector 카운터가 '줄어든 것처럼' 보인다."
note "종료된 인스턴스의 누적 카운터가 사라지기 때문이다 (Prometheus 카운터는 프로세스 수명 기준)."
note "실제 운영에서는 Prometheus 가 인스턴스별 시계열을 따로 보관해 이런 착시가 없다."
echo
note "다음에 볼 것:"
note "  bash scripts/flush-windows.sh && bash scripts/batch.sh && bash scripts/recon.sh"
