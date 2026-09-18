"""대사 (reconciliation) — 실시간 집계 vs 확정 집계.

Redis 의 분단위 실시간 합계(Flink 산출)와 PostgreSQL 의 확정 집계(Spark 배치 산출)를
캠페인별로 비교하고, 차이율과 원인 후보를 한 표로 뽑아 postgres 에 남긴다.

두 값은 원래 다르다. 다른 게 정상이고, 얼마나 왜 다른지가 관심사다.
차이의 원인은 셋 중 하나(또는 조합)다.

  A. 지연 반영 (late)
     워터마크를 지나 도착한 이벤트는 Flink 윈도우에서 버려졌다(late.events 로 갔다).
     배치는 event_time 만 보므로 전부 집계에 넣는다.  -> 확정 > 실시간

  B. 중복 제거 범위 (dedup scope)
     Flink 는 상태 TTL 1시간 안에서만 event_id 중복을 본다.
     배치는 전체 범위를 본다.                        -> 확정 < 실시간

  C. SSAI 이중경로 (ssai)
     같은 ad_request_id 의 impression 이 client/server 두 경로로 온 것.
     event_id 가 달라 Flink 는 못 걸러 둘 다 센다.
     배치는 ad_request_id 단위로 하나만 채택한다.    -> 확정 < 실시간

Redis 는 redis-writer 의 /agg/summary 엔드포인트로 읽는다.
(Spark 컨테이너에 redis 클라이언트를 넣지 않기 위해서다. HTTP 면 충분하다.)

실행: bash scripts/recon.sh
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import build_spark, hr, jdbc_execute, read_jdbc, write_jdbc  # noqa: E402

REDIS_API = os.getenv("REDIS_API", "http://redis-writer:8099")
DT = os.getenv("RECON_DT") or datetime.now(timezone.utc).strftime("%Y-%m-%d")

# 차이율이 이보다 크면 "조사 필요" 로 표시한다.
WARN_RATE = float(os.getenv("RECON_WARN_RATE", "0.05"))


def fetch_realtime(dt):
    url = f"{REDIS_API}/agg/summary?dt={dt}"
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())


def classify(diff, ssai, late, residual, rt, bt):
    """차이를 원인별로 분해한다.

    확정(batch) = 실시간(realtime) + 지연반영 - SSAI - 장기중복  (+ 잔차)

    부호 규약
      late  : 실시간이 버렸고 배치는 넣었다        -> 확정을 키운다 (+)
      ssai  : 실시간이 셌고 배치는 뺐다            -> 확정을 줄인다 (-)
      잔차  : 위로 설명 안 되는 나머지. 대개 관측 구간 불일치.
    """
    if rt == 0 and bt > 0:
        return "실시간 미집계(Flink 잡이 그 구간에 없었거나 윈도우 미확정)"
    if bt == 0 and rt > 0:
        return "배치 미적재(archive 잡이 그 구간에 없었거나 Parquet 커밋 전)"

    parts = []
    if late:
        parts.append(f"지연반영 +{late}")
    if ssai:
        parts.append(f"SSAI이중경로 -{ssai}")
    if residual > 0:
        # 확정이 더 크다. 실시간이 아직 못 낸 구간이 있다.
        parts.append(f"잔차 +{residual}(실시간 미확정 윈도우 - "
                     f"부하가 멈추면 워터마크도 멈춘다. scripts/flush-windows.sh)")
    elif residual < 0:
        # 실시간이 더 크다. late 로 표식된 것 중 일부는 윈도우 마감 전에 도착해
        # 실시간에도 이미 반영됐다는 뜻이다 (지연반영 항을 그만큼 과대계상한 것).
        parts.append(f"잔차 {residual}(late 표식이지만 윈도우 마감 전 도착해 "
                     f"실시간에도 이미 반영된 분)")
    return " , ".join(parts) if parts else "차이 없음"


def main():
    spark = build_spark("reconcile")
    spark.sparkContext.setLogLevel("WARN")

    hr(f"대사 대상일: {DT}")

    # ------------------------------------------------------------- 실시간
    try:
        rt = fetch_realtime(DT)
    except Exception as e:
        print(f"  redis-writer 조회 실패: {e}")
        print(f"  {REDIS_API}/agg/summary 가 뜨는지 확인할 것.")
        spark.stop()
        return 1
    rt_camps = rt.get("campaigns", {})
    late_total = int(rt.get("late_total", 0))
    print(f"  실시간(Redis) 캠페인 {len(rt_camps)}개, late 누계 {late_total}, "
          f"alert 누계 {rt.get('alerts_total', 0)}")

    # ------------------------------------------------------------- 확정
    batch = (read_jdbc(spark, "daily_settlement")
             .filter(F.col("dt") == F.lit(DT))
             .select("campaign_id", "impressions", "clicks", "completes",
                     "requests", "raw_impressions", "dupes_removed",
                     "late_impressions", "amount"))
    b_rows = {r["campaign_id"]: r.asDict() for r in batch.collect()}
    print(f"  확정(PostgreSQL) 캠페인 {len(b_rows)}개")

    if not rt_camps and not b_rows:
        print("\n  양쪽 다 비어 있다. 부하 -> Flink -> archive -> batch 순으로 먼저 돌릴 것.")
        spark.stop()
        return 1

    # ------------------------------------------------------------- 비교
    hr("실시간 vs 확정 (impression 기준)")
    print("  실시간 = Redis 분단위 합계 (Flink: event_id 중복만 제거, SSAI 는 남아 있음)")
    print("  확정   = daily_settlement (Spark: 전체범위 중복 제거 + SSAI 정리 + 지연 포함)")
    print()
    print(f"  {'campaign':<11}{'실시간':>8}{'확정':>8}{'차이':>7}{'차이율':>9}   원인 분해")
    print("  " + "-" * 104)

    out = []
    now = datetime.now(timezone.utc)
    for cid in sorted(set(rt_camps) | set(b_rows)):
        r_imp = int(rt_camps.get(cid, {}).get("impressions", 0))
        b = b_rows.get(cid, {})
        b_imp = int(b.get("impressions") or 0)
        ssai = int(b.get("raw_impressions") or 0) - b_imp
        late = int(b.get("late_impressions") or 0)
        diff = b_imp - r_imp
        # 설명되는 부분과 잔차를 나눈다.
        residual = diff - (late - ssai)
        base = max(r_imp, b_imp, 1)
        rate = diff / base
        cause = classify(diff, ssai, late, residual, r_imp, b_imp)
        flag = "  <=" if abs(rate) > WARN_RATE else ""
        print(f"  {cid:<11}{r_imp:>8}{b_imp:>8}{diff:>7}{rate:>8.2%}   {cause}{flag}")
        out.append(Row(
            dt=datetime.strptime(DT, "%Y-%m-%d").date(),
            campaign_id=cid,
            realtime_impressions=r_imp,
            batch_impressions=b_imp,
            diff=diff,
            diff_rate=round(rate, 4),
            likely_cause=cause,
            run_at=now,
        ))

    print("  " + "-" * 104)
    tr = sum(int(v.get("impressions", 0)) for v in rt_camps.values())
    tb = sum(int(v.get("impressions") or 0) for v in b_rows.values())
    t_ssai = sum(int(v.get("raw_impressions") or 0) - int(v.get("impressions") or 0)
                 for v in b_rows.values())
    t_late = sum(int(v.get("late_impressions") or 0) for v in b_rows.values())
    td = tb - tr
    print(f"  {'합계':<11}{tr:>8}{tb:>8}{td:>7}{(td / max(tr, tb, 1)):>8.2%}"
          f"   지연반영 +{t_late} , SSAI이중경로 -{t_ssai} , 잔차 {td - (t_late - t_ssai):+d}")
    print()
    print("  해석: 차이가 0 이 아닌 게 정상이다.")
    print("        확정 > 실시간 이면 실시간이 놓친 지연 이벤트가 있었다는 뜻이고,")
    print("        확정 < 실시간 이면 실시간이 SSAI/장기 중복을 과다 계상했다는 뜻이다.")
    print("        정산은 확정값 기준이고, 실시간은 운영 모니터링용이다.")

    # ------------------------------------------------------------- 저장
    hr("reconciliation 테이블 기록")
    jdbc_execute(spark,
                 "ALTER TABLE reconciliation ADD COLUMN IF NOT EXISTS dt DATE")
    df = spark.createDataFrame(out).select(
        "run_at", "dt", "campaign_id", "realtime_impressions",
        "batch_impressions", "diff", "diff_rate", "likely_cause")
    write_jdbc(df, "reconciliation", mode="append", truncate=False)
    print(f"  {len(out)}행 추가 (append - 실행 이력이 쌓인다)")
    print("  조회: docker compose exec postgres psql -U ads -d adplatform "
          "-c 'select * from reconciliation order by run_at desc limit 10;'")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
