"""대사 (reconciliation) — 실시간 집계 vs 확정 집계.

Redis 의 분단위 실시간 합계(Flink 산출)와 PostgreSQL 의 확정 집계(Spark 배치 산출)를
캠페인별로 비교하고, 차이를 원인별로 분해해 postgres 에 남긴다.

두 값은 원래 다르다. 다른 게 정상이고, 얼마나 왜 다른지가 관심사다.
차이의 원인은 셋 중 하나(또는 조합)다.

  A. 지연 반영 (late)
     속한 1분 창이 이미 닫힌 뒤 도착한 이벤트는 Flink 윈도우가 버렸다.
     Flink 가 그 이벤트를 같은 조건으로 판정해 late_dropped 테이블에 남긴다 (event_id 기본키).
     배치는 event_time 만 보므로 전부 집계에 넣는다.  -> 확정 > 실시간

  B. 중복 제거 범위 (dedup scope)
     Flink 는 상태 TTL 1시간 안에서만 event_id 중복을 본다.
     배치는 전체 범위를 본다.                        -> 확정 < 실시간

  C. SSAI 이중경로 (ssai)
     같은 ad_request_id 의 impression 이 client/server 두 경로로 온 것.
     event_id 가 달라 Flink 는 못 걸러 둘 다 센다.
     배치는 ad_request_id 단위로 하나만 채택한다.    -> 확정 < 실시간

A 와 C 를 정확히 세므로 식이 딱 맞는다.

  확정 - 실시간 = 지연반영(late_dropped) - SSAI(raw - 확정) + 잔차

  잔차 = 둘로 설명되지 않는 나머지. 0 이 정상이고, 0 이 아니면 아래 중 하나다.
    + : 실시간에 아직 안 나온 창이 있다 (부하가 멈춰 워터마크가 멈춤 -> flush-windows.sh)
        또는 그 구간에 Flink 잡이 없었다
    - : 실시간이 더 셌다. Flink 중복제거 TTL(1시간)을 넘긴 재전송(B),
        또는 그 구간에 archive 잡이 없어 배치에 원본이 없다

Redis 는 redis-writer 의 /agg/summary 엔드포인트로 읽는다.
(Spark 컨테이너에 redis 클라이언트를 넣지 않기 위해서다. HTTP 면 충분하다.)

실행: bash scripts/recon.sh                                       # 오늘(UTC) 하루
      RECON_FROM=2026-09-21T08:30 RECON_TO=2026-09-21T09:00 \\
      bash scripts/recon.sh                                       # 구간 [FROM, TO) UTC, 분 단위

구간 대사는 분 단위 확정 집계(minute_settlement)를 쓴다.
다른 실행의 데이터가 섞인 날에 "이번 실행 구간만" 맞춰 볼 때 쓴다.
"""
import json
import os
import sys
import urllib.request
from datetime import datetime, timedelta, timezone

from pyspark.sql import Row
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import build_spark, hr, jdbc_execute, read_jdbc, write_jdbc  # noqa: E402

REDIS_API = os.getenv("REDIS_API", "http://redis-writer:8099")
DT = os.getenv("RECON_DT") or datetime.now(timezone.utc).strftime("%Y-%m-%d")
R_FROM = os.getenv("RECON_FROM") or None
R_TO = os.getenv("RECON_TO") or None

# 차이율이 이보다 크면 "조사 필요" 로 표시한다.
WARN_RATE = float(os.getenv("RECON_WARN_RATE", "0.05"))


def parse_utc(s):
    """'2026-09-21T08:30' (UTC) -> 분 단위로 자른 aware datetime"""
    t = datetime.fromisoformat(s)
    t = t.replace(tzinfo=timezone.utc) if t.tzinfo is None else t.astimezone(timezone.utc)
    return t.replace(second=0, microsecond=0)


def fetch_realtime(t0, t1):
    url = f"{REDIS_API}/agg/summary?from={int(t0.timestamp())}&to={int(t1.timestamp())}"
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read().decode())


def fetch_late_dropped(spark, t0, t1):
    """실시간 윈도우가 버린 impression 을 캠페인별로 센다. event_time(UTC) 이 [t0, t1) 인 것."""
    try:
        df = read_jdbc(spark, "late_dropped")
    except Exception as e:
        print(f"  late_dropped 조회 실패 ({e.__class__.__name__}) "
              f"- flink-submit.sh 로 잡을 올리면 테이블이 생긴다. 지연반영을 0 으로 둔다.")
        return {}
    lo, hi = t0.replace(tzinfo=None), t1.replace(tzinfo=None)   # 테이블은 UTC 기준 TIMESTAMP
    rows = (df.filter((F.col("kind") == "impression")
                      & (F.col("event_time") >= F.lit(lo))
                      & (F.col("event_time") < F.lit(hi)))
              .groupBy("campaign_id").count().collect())
    return {r["campaign_id"]: int(r["count"]) for r in rows}


def fetch_batch(spark, ranged, t0, t1):
    if ranged:
        df = (read_jdbc(spark, "minute_settlement")
              .filter((F.col("minute_ts") >= F.lit(t0)) & (F.col("minute_ts") < F.lit(t1)))
              .groupBy("campaign_id")
              .agg(F.sum("impressions").alias("impressions"),
                   F.sum("raw_impressions").alias("raw_impressions")))
    else:
        df = (read_jdbc(spark, "daily_settlement")
              .filter(F.col("dt") == F.lit(DT))
              .select("campaign_id", "impressions", "raw_impressions"))
    return {r["campaign_id"]: r.asDict() for r in df.collect() if r["campaign_id"]}


def classify(ssai, late, residual, rt, bt):
    """차이를 원인별로 분해한다.

    확정(batch) = 실시간(realtime) + 지연반영 - SSAI + 잔차

    부호 규약
      late  : 실시간이 버렸고 배치는 넣었다        -> 확정을 키운다 (+)
      ssai  : 실시간이 셌고 배치는 뺐다            -> 확정을 줄인다 (-)
      잔차  : 위로 설명 안 되는 나머지. 0 이 정상.
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
        parts.append(f"잔차 +{residual}(실시간 미확정 창 - 부하가 멈추면 워터마크도 멈춘다. "
                     f"scripts/flush-windows.sh / 또는 그 구간에 Flink 잡이 없었음)")
    elif residual < 0:
        parts.append(f"잔차 {residual}(TTL 1시간을 넘긴 재전송을 실시간이 중복으로 셌거나 "
                     f"그 구간에 archive 잡이 없어 원본 누락)")
    return " , ".join(parts) if parts else "차이 없음"


def main():
    spark = build_spark("reconcile")
    spark.sparkContext.setLogLevel("WARN")

    ranged = bool(R_FROM and R_TO)
    if ranged:
        t0, t1 = parse_utc(R_FROM), parse_utc(R_TO)
        scope = f"{t0:%Y-%m-%d %H:%M} ~ {t1:%H:%M} UTC"
    else:
        t0 = datetime.strptime(DT, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        t1 = t0 + timedelta(days=1)
        scope = f"{DT} (UTC 하루)"
    hr(f"대사 범위: {scope}")

    # ------------------------------------------------------------- 실시간
    try:
        rt = fetch_realtime(t0, t1)
    except Exception as e:
        print(f"  redis-writer 조회 실패: {e}")
        print(f"  {REDIS_API}/agg/summary 가 뜨는지 확인할 것.")
        spark.stop()
        return 1
    rt_camps = rt.get("campaigns", {})
    print(f"  실시간(Redis) 캠페인 {len(rt_camps)}개")

    # ------------------------------------------------------------- 확정
    b_rows = fetch_batch(spark, ranged, t0, t1)
    print(f"  확정(PostgreSQL {'minute_settlement' if ranged else 'daily_settlement'}) "
          f"캠페인 {len(b_rows)}개")

    # ------------------------------------------------------------- 실시간이 버린 것
    late_by = fetch_late_dropped(spark, t0, t1)
    print(f"  실시간 윈도우가 버린 impression (late_dropped) {sum(late_by.values())}건")

    if not rt_camps and not b_rows:
        print("\n  양쪽 다 비어 있다. 부하 -> Flink -> archive -> batch 순으로 먼저 돌릴 것.")
        spark.stop()
        return 1

    # ------------------------------------------------------------- 비교
    hr("실시간 vs 확정 (impression 기준)")
    print("  실시간 = Redis 분단위 합계 (Flink: event_id 중복만 제거, SSAI 는 남아 있음, 지각은 버림)")
    print("  확정   = Spark (전체범위 중복 제거 + SSAI 정리 + 지각 포함)")
    print()
    print(f"  {'campaign':<11}{'실시간':>8}{'확정':>8}{'차이':>7}{'차이율':>9}   원인 분해")
    print("  " + "-" * 104)

    out = []
    now = datetime.now(timezone.utc)
    t_rt = t_bt = t_late = t_ssai = 0
    for cid in sorted(set(rt_camps) | set(b_rows)):
        r_imp = int(rt_camps.get(cid, {}).get("impressions", 0))
        b = b_rows.get(cid, {})
        b_imp = int(b.get("impressions") or 0)
        ssai = int(b.get("raw_impressions") or 0) - b_imp
        late = late_by.get(cid, 0)
        diff = b_imp - r_imp
        residual = diff - (late - ssai)
        rate = diff / max(r_imp, b_imp, 1)
        cause = classify(ssai, late, residual, r_imp, b_imp)
        flag = "  <=" if abs(rate) > WARN_RATE else ""
        print(f"  {cid:<11}{r_imp:>8}{b_imp:>8}{diff:>7}{rate:>8.2%}   {cause}{flag}")
        t_rt, t_bt, t_late, t_ssai = t_rt + r_imp, t_bt + b_imp, t_late + late, t_ssai + ssai
        out.append(Row(
            dt=t0.date(),
            campaign_id=cid,
            realtime_impressions=r_imp,
            batch_impressions=b_imp,
            diff=diff,
            diff_rate=round(rate, 4),
            likely_cause=cause,
            run_at=now,
            scope=scope,
        ))

    print("  " + "-" * 104)
    td = t_bt - t_rt
    print(f"  {'합계':<11}{t_rt:>8}{t_bt:>8}{td:>7}{(td / max(t_rt, t_bt, 1)):>8.2%}"
          f"   지연반영 +{t_late} , SSAI이중경로 -{t_ssai} , 잔차 {td - (t_late - t_ssai):+d}")
    print()
    print("  해석: 차이가 0 이 아닌 건 정상이다. 잔차가 0 이 아니면 원인을 봐야 한다.")
    print("        지연반영 = 실시간 윈도우가 실제로 버린 건수 (Flink 가 남긴 late_dropped)")
    print("        SSAI이중경로 = 실시간은 둘 다 셌고 배치는 하나만 남긴 건수")
    print("        정산은 확정값 기준이고, 실시간은 운영 모니터링용이다.")

    # ------------------------------------------------------------- 저장
    hr("reconciliation 테이블 기록")
    jdbc_execute(spark, "ALTER TABLE reconciliation ADD COLUMN IF NOT EXISTS dt DATE")
    jdbc_execute(spark, "ALTER TABLE reconciliation ADD COLUMN IF NOT EXISTS scope TEXT")
    df = spark.createDataFrame(out).select(
        "run_at", "dt", "campaign_id", "realtime_impressions",
        "batch_impressions", "diff", "diff_rate", "likely_cause", "scope")
    write_jdbc(df, "reconciliation", mode="append", truncate=False)
    print(f"  {len(out)}행 추가 (append - 실행 이력이 쌓인다)")
    print("  조회: docker compose exec postgres psql -U ads -d adplatform "
          "-c 'select * from reconciliation order by run_at desc limit 10;'")

    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
