#!/usr/bin/env bash
# ---------------------------------------------------------------------------
# E2E 테스트 — 전체 스택을 띄우고 이벤트를 넣어 수집부터 대사까지 정확한 숫자로 확인한다.
#
#   bash scripts/e2e.sh                 # 약 7~8분 (이미지가 빌드돼 있을 때)
#   E2E_DOWN=1 bash scripts/e2e.sh      # 끝나면 스택을 내린다
#
# 순서
#   1. 기동 + 헬스          docker compose up, scripts/health.sh
#   2. Flink 잡 새로 제출    기존 잡을 취소하고 realtime / archive 를 다시 올린다
#   3. 배경 부하             생성기 (기본 20명, 180초) — 워터마크가 계속 움직이게
#   4. 테스트 이벤트 주입     부하 100초째에 전용 캠페인으로 정해진 이벤트를 넣는다
#   5. 창 닫기 → 배치 → 구간 대사
#   6. 검사                  tests/e2e/pipeline_checks.py check (실패 1개라도 있으면 exit 1)
#
# 검사 스크립트는 tracer 컨테이너 안에서 돈다 (Kafka/Redis/Postgres/MinIO 클라이언트가 다 있다).
# 무엇을 넣고 무엇을 기대하는지는 그 파일 맨 위 주석에 있다.
# ---------------------------------------------------------------------------
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

USERS="${E2E_USERS:-20}"
LOAD_SEC="${E2E_LOAD_SEC:-180}"
SEND_AT="${E2E_SEND_AT:-100}"
RUN_ID="$(date -u +%m%d%H%M%S)"
OUT=data/e2e
mkdir -p "$OUT"
T_START=$(date +%s)

step() { printf '\n\033[1m== %s\033[0m  (+%ss)\n' "$*" "$(( $(date +%s) - T_START ))"; }
die()  { printf '\n\033[31m[e2e] 실패: %s\033[0m\n' "$*"; finish 1; }
finish() {
  if [[ "${E2E_DOWN:-0}" == "1" ]]; then
    docker compose --profile batch --profile load down --remove-orphans >/dev/null 2>&1
  fi
  exit "$1"
}
flink_running() {
  curl -fsS http://localhost:8181/jobs/overview 2>/dev/null | python -c "
import sys, json
names = {j['name'] for j in json.load(sys.stdin)['jobs'] if j['state'] == 'RUNNING'}
sys.exit(0 if {'ott-ads-realtime', 'ott-ads-archive'} <= names else 1)"
}
realtime_watermark_ms() {   # 실시간 잡 윈도우 집계 연산자의 현재 워터마크
  python - <<'EOF'
import json, urllib.request
get = lambda p: json.load(urllib.request.urlopen("http://localhost:8181" + p, timeout=5))
jid = next(j["jid"] for j in get("/jobs/overview")["jobs"]
           if j["name"] == "ott-ads-realtime" and j["state"] == "RUNNING")
vid = next(v["id"] for v in get(f"/jobs/{jid}")["vertices"] if v["name"].startswith("GlobalWindowAggregate"))
print(min(int(w["value"]) for w in get(f"/jobs/{jid}/vertices/{vid}/watermarks")))
EOF
}
utc_minute() {  # epoch ms -> 2026-09-21T08:31 (분 단위 내림, UTC)
  python -c "import datetime as d,sys; print(d.datetime.fromtimestamp(int(sys.argv[1])//60000*60, d.timezone.utc).strftime('%Y-%m-%dT%H:%M'))" "$1"
}

step "1. 기동 + 헬스"
[[ -f .env ]] || cp .env.example .env
docker compose up -d --build >"$OUT/up.log" 2>&1 || die "docker compose up (로그: $OUT/up.log)"
bash scripts/health.sh || die "헬스 체크"

step "2. Flink 잡 새로 제출"
bash scripts/flink-cancel.sh >/dev/null
for _ in $(seq 1 30); do   # 취소된 잡의 슬롯이 풀릴 때까지
  curl -fsS http://localhost:8181/overview 2>/dev/null | grep -q '"slots-available":2' && break
  sleep 2
done
bash scripts/flink-submit.sh >"$OUT/flink-submit.log" 2>&1 || die "flink-submit (로그: $OUT/flink-submit.log)"
bash scripts/flink-archive.sh >"$OUT/flink-archive.log" 2>&1 || die "flink-archive (로그: $OUT/flink-archive.log)"
for _ in $(seq 1 30); do flink_running && break; sleep 2; done
flink_running || die "Flink 잡이 RUNNING 이 아니다"
echo "  realtime / archive RUNNING"

step "3. 배경 부하 (${USERS}명, ${LOAD_SEC}초)"
USERS="$USERS" DURATION="$LOAD_SEC" bash scripts/load.sh >"$OUT/load.log" 2>&1 &
LOAD_PID=$!
sleep "$SEND_AT"

step "4. 테스트 이벤트 주입 (run_id=$RUN_ID)"
STATE=$(docker compose exec -T tracer python - send "$RUN_ID" < tests/e2e/pipeline_checks.py | tail -1)
[[ "$STATE" == \{* ]] || die "이벤트 주입 (출력: $STATE)"
echo "$STATE" > "$OUT/state.json"
LATE_TS=$(python -c "import json,sys; print(json.load(open(sys.argv[1]))['late_ts'])" "$OUT/state.json")
wait "$LOAD_PID" || die "부하 생성기 (로그: $OUT/load.log)"

step "5. 창 닫기 → 배치 → 구간 대사"
bash scripts/flush-windows.sh >"$OUT/flush.log" 2>&1
sleep 5   # redis-writer 가 마지막 agg.minute 을 반영할 시간
# 대사 구간 = [지연 이벤트의 분, 워터마크가 이미 닫은 마지막 분 경계)
# 끝을 워터마크로 잡아야 "아직 안 닫힌 창" 이 구간에 들어가지 않는다 (들어가면 잔차 + 가 난다).
RECON_FROM=$(utc_minute "$LATE_TS")
RECON_TO=$(utc_minute "$(realtime_watermark_ms)")
echo "  대사 구간: $RECON_FROM ~ $RECON_TO (UTC)"
bash scripts/batch.sh >"$OUT/batch.log" 2>&1 || die "배치 (로그: $OUT/batch.log)"
RECON_FROM="$RECON_FROM" RECON_TO="$RECON_TO" bash scripts/recon.sh >"$OUT/recon.log" 2>&1 \
  || die "대사 (로그: $OUT/recon.log)"
sed -n '/campaign /,/^  합계/p' "$OUT/recon.log"

step "6. 검사"
STATE_B64=$(python -c "import base64,sys; print(base64.b64encode(open(sys.argv[1],'rb').read()).decode())" "$OUT/state.json")
docker compose exec -T tracer python - check "$STATE_B64" < tests/e2e/pipeline_checks.py
rc=$?
printf '\n[e2e] %s  (총 %ss, 로그: %s/)\n' "$([[ $rc -eq 0 ]] && echo 통과 || echo 실패)" \
  "$(( $(date +%s) - T_START ))" "$OUT"
finish "$rc"
