#!/usr/bin/env bash
# Spark 정산 배치. spark 컨테이너를 그때만 띄운다 (batch 프로파일).
set -uo pipefail
export MSYS_NO_PATHCONV=1
export MSYS2_ARG_CONV_EXCL="*"
cd "$(dirname "$0")/.."

JOB="${1:-batch_settlement.py}"
echo "[batch] $JOB 실행 (spark 컨테이너 on-demand)"
exec docker compose --profile batch run --rm \
  -e SSAI_SOURCE_PRIORITY="${SSAI_SOURCE_PRIORITY:-server,client}" \
  -e RECON_DT -e RECON_FROM -e RECON_TO -e RECON_WARN_RATE \
  --entrypoint /opt/spark/bin/spark-submit \
  spark --master "local[*]" --driver-memory 1g "/opt/spark-jobs/$JOB"
