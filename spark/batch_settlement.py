"""확정 집계 (일/캠페인별 정산).

MinIO 의 Parquet 원본을 전부 읽어 실시간(Flink)이 못 한 두 가지를 한다.

  1) event_id 기준 전체 범위 중복 제거
     Flink 는 상태 TTL 1시간 안에서만 중복을 본다. 1시간 뒤에 도착한 재전송,
     fallback 파일 재적재, Outbox 재발행은 실시간에서 그냥 통과한다.
     배치는 전체 데이터를 보므로 전부 잡는다.

  2) SSAI 이중경로 처리
     같은 ad_request_id 에 impression 이 둘 이상이면 하나만 채택한다.
     event_id 가 서로 다르므로 1)로는 절대 안 걸린다.
     채택 우선순위는 source 필드로 정한다 (기본 server > client).
     SSAI 스티처가 서버에서 직접 찍는 비콘이 광고차단의 영향을 안 받아 더 신뢰도가 높다.

그 뒤 campaign_rates 의 CPM 과 조인해 정산 금액을 계산하고
PostgreSQL daily_settlement 에 기록한다.

실행: bash scripts/batch.sh
"""
import os
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from common import (EVENTS_PATH, build_spark, hr, jdbc_execute, read_jdbc,  # noqa: E402
                    write_jdbc)

# SSAI 중복에서 어느 경로를 채택할지. 앞에 올수록 우선.
SSAI_PRIORITY = os.getenv("SSAI_SOURCE_PRIORITY", "server,client").split(",")

# 정산 대상이 아닌 campaign_id (행동 이벤트 / 노필 / 결정 전 요청)
NON_CAMPAIGN = ["none", "nofill", "pending"]


def main():
    spark = build_spark("batch_settlement")
    spark.sparkContext.setLogLevel("WARN")

    hr("1) 원본 Parquet 읽기")
    try:
        raw = spark.read.parquet(EVENTS_PATH)
    except Exception as e:
        print(f"  읽기 실패: {e}")
        print("  MinIO 에 아직 데이터가 없을 수 있다. 먼저 archive 잡과 부하를 돌릴 것:")
        print("    bash scripts/flink-archive.sh && bash scripts/load.sh")
        spark.stop()
        return 1

    raw = raw.filter(F.col("event_id").isNotNull())
    raw_rows = raw.count()
    if raw_rows == 0:
        print("  Parquet 은 있는데 행이 없다. 종료.")
        spark.stop()
        return 1
    print(f"  경로       : {EVENTS_PATH}")
    print(f"  총 행수    : {raw_rows:,}")
    parts = raw.select("dt", "hour").distinct().orderBy("dt", "hour").collect()
    print(f"  파티션     : {', '.join(f'{r.dt}/{r.hour}' for r in parts[:12])}"
          + (" ..." if len(parts) > 12 else ""))

    # ---------------------------------------------------------------- 2)
    hr("2) event_id 기준 전체 범위 중복 제거")
    # dropDuplicates 는 어느 행이 남는지 비결정적이다.
    # 재실행 결과를 같게 만들려고 event_time 이 가장 이른 행을 남긴다.
    w_evt = Window.partitionBy("event_id").orderBy(
        F.col("event_time").asc_nulls_last(), F.col("source_topic").asc()
    )
    dedup = (raw.withColumn("_rn", F.row_number().over(w_evt))
                .filter(F.col("_rn") == 1).drop("_rn"))
    dedup_rows = dedup.count()
    print(f"  중복 제거 전 : {raw_rows:,}")
    print(f"  중복 제거 후 : {dedup_rows:,}")
    print(f"  제거된 중복  : {raw_rows - dedup_rows:,}"
          f"  ({(raw_rows - dedup_rows) / raw_rows * 100:.2f}%)")
    print("  -> 생성기의 재전송 + Outbox 재발행 + fallback 재적재분이 여기서 접힌다.")

    dedup = dedup.withColumn("dt_date", F.to_date(F.col("event_time")))
    dedup.cache()

    # ---------------------------------------------------------------- 3)
    hr("3) SSAI 이중경로 처리 (ad_request_id 단위, source 우선순위)")
    imp_all = dedup.filter(F.col("event_type") == "impression") \
                   .filter(~F.col("campaign_id").isin(NON_CAMPAIGN))
    raw_imp = imp_all.count()

    prio = F.lit(len(SSAI_PRIORITY) + 1)
    for i, src in enumerate(reversed(SSAI_PRIORITY)):
        prio = F.when(F.col("source") == src.strip(), F.lit(len(SSAI_PRIORITY) - 1 - i)).otherwise(prio)

    # ad_request_id 가 비면 event_id 로 대체한다. 안 그러면 NULL 끼리 한 그룹이 된다.
    grp = F.coalesce(F.col("ad_request_id"), F.col("event_id"))
    w_ssai = Window.partitionBy(grp).orderBy(
        prio.asc(), F.col("event_time").asc_nulls_last(), F.col("event_id").asc()
    )
    imp = (imp_all.withColumn("_rn", F.row_number().over(w_ssai))
                  .filter(F.col("_rn") == 1).drop("_rn"))
    imp.cache()
    kept_imp = imp.count()
    print(f"  우선순위     : {' > '.join(s.strip() for s in SSAI_PRIORITY)}")
    print(f"  중복 제거 전 : {raw_imp:,} impression")
    print(f"  채택         : {kept_imp:,}")
    print(f"  버린 이중경로: {raw_imp - kept_imp:,}")
    print("  -> event_id 가 서로 달라서 2)에서는 절대 안 걸리던 것들이다.")
    if raw_imp:
        by_src = imp.groupBy("source").count().orderBy(F.desc("count")).collect()
        print("  채택된 것의 source 분포: "
              + ", ".join(f"{r['source']}={r['count']}" for r in by_src))

    # ---------------------------------------------------------------- 4)
    hr("4) 일/캠페인별 집계")
    base = dedup.filter(~F.col("campaign_id").isin(NON_CAMPAIGN)) \
                .filter(F.col("campaign_id").isNotNull())

    clicks = (base.filter(F.col("event_type") == "click")
                  .groupBy("dt_date", "campaign_id").agg(F.count("*").alias("clicks")))
    completes = (base.filter((F.col("event_type") == "quartile") & (F.col("quartile") == "complete"))
                     .groupBy("dt_date", "campaign_id").agg(F.count("*").alias("completes")))
    # 분모는 서버가 확정해 실제로 채운 광고. Flink 의 정합성 지표와 같은 정의.
    requests = (base.filter((F.col("event_type") == "ad_response") & (F.col("fill") == True))  # noqa: E712
                    .groupBy("dt_date", "campaign_id").agg(F.count("*").alias("requests")))

    imp_agg = imp.groupBy("dt_date", "campaign_id").agg(F.count("*").alias("impressions"))
    # raw_impressions = event_id 중복 제거 후 / SSAI 제거 전.
    # 실시간(Flink)이 세는 값과 같은 지점이라 대사의 기준점이 된다.
    raw_imp_agg = imp_all.groupBy("dt_date", "campaign_id").agg(F.count("*").alias("raw_impressions"))

    # impression 한정 event_id 중복 건수. (전체 이벤트가 아니라 impression 만 세야
    #  대사 표에서 impression 차이와 같은 축으로 비교된다.)
    imp_raw_rows = (raw.withColumn("dt_date", F.to_date(F.col("event_time")))
                       .filter(F.col("event_type") == "impression")
                       .filter(~F.col("campaign_id").isin(NON_CAMPAIGN))
                       .filter(F.col("campaign_id").isNotNull())
                       .groupBy("dt_date", "campaign_id").agg(F.count("*").alias("imp_rows_raw")))

    # 생성기가 지연을 심은 impression. Flink 워터마크(기본 10초)보다 훨씬 늦은
    # 30~60초 과거라 실시간 윈도우에서는 사실상 전부 버려졌다(late.events 로 갔다).
    # 배치는 event_time 만 보므로 전부 집계에 들어간다 -> 확정이 더 커지는 쪽 원인.
    late_imp = (imp.filter(F.col("late_flag") == True)  # noqa: E712
                   .groupBy("dt_date", "campaign_id").agg(F.count("*").alias("late_impressions")))

    agg = (raw_imp_agg
           .join(imp_agg, ["dt_date", "campaign_id"], "left")
           .join(clicks, ["dt_date", "campaign_id"], "left")
           .join(completes, ["dt_date", "campaign_id"], "left")
           .join(requests, ["dt_date", "campaign_id"], "left")
           .join(imp_raw_rows, ["dt_date", "campaign_id"], "left")
           .join(late_imp, ["dt_date", "campaign_id"], "left")
           .fillna(0, ["impressions", "clicks", "completes", "requests",
                       "raw_impressions", "imp_rows_raw", "late_impressions"]))

    # ---------------------------------------------------------------- 5)
    hr("5) CPM 단가 조인 + 정산 금액")
    rates = read_jdbc(spark, "campaign_rates").select("campaign_id", "cpm")
    final = (agg.join(rates, "campaign_id", "left")
                .withColumn("cpm", F.coalesce(F.col("cpm"), F.lit(0.0)))
                # CPM = 1000 임프레션당 단가
                .withColumn("amount", F.round(F.col("impressions") / F.lit(1000.0) * F.col("cpm"), 2))
                .withColumn("dupes_removed", F.col("imp_rows_raw") - F.col("raw_impressions"))
                .withColumn("computed_at", F.current_timestamp())
                .select(F.col("dt_date").alias("dt"), "campaign_id", "requests", "impressions",
                        "clicks", "completes", "raw_impressions", "dupes_removed",
                        "late_impressions", "cpm", "amount", "computed_at")
                .orderBy("dt", "campaign_id"))

    rows = final.collect()
    print(f"  {'dt':<11}{'campaign':<11}{'req':>7}{'imp':>7}{'raw_imp':>9}"
          f"{'ssai':>6}{'dup':>6}{'late':>6}{'clk':>5}{'cmpl':>6}{'cpm':>10}{'amount':>12}")
    print("  " + "-" * 98)
    total = 0.0
    for r in rows:
        ssai = (r["raw_impressions"] or 0) - (r["impressions"] or 0)
        total += float(r["amount"] or 0)
        print(f"  {str(r['dt']):<11}{r['campaign_id']:<11}{r['requests']:>7}{r['impressions']:>7}"
              f"{r['raw_impressions']:>9}{ssai:>6}{r['dupes_removed']:>6}{r['late_impressions']:>6}"
              f"{r['clicks']:>5}{r['completes']:>6}{float(r['cpm']):>10,.0f}"
              f"{float(r['amount'] or 0):>12,.2f}")
    print("  " + "-" * 98)
    print(f"  {'합계':<89}{total:>12,.2f} KRW")
    print()
    print("  raw_imp = event_id 중복 제거 후 / SSAI 제거 전  -> 실시간(Flink)이 세는 값과 같은 지점")
    print("  imp     = 거기서 SSAI 이중경로까지 정리한 확정값 -> 정산 기준")

    # ---------------------------------------------------------------- 6)
    hr("6) PostgreSQL daily_settlement 기록")
    # 기존 볼륨에도 컬럼이 생기도록 여기서 맞춘다 (init SQL 은 최초 1회만 돈다).
    jdbc_execute(spark, "ALTER TABLE daily_settlement "
                        "ADD COLUMN IF NOT EXISTS dupes_removed BIGINT NOT NULL DEFAULT 0")
    jdbc_execute(spark, "ALTER TABLE daily_settlement "
                        "ADD COLUMN IF NOT EXISTS late_impressions BIGINT NOT NULL DEFAULT 0")
    # 배치는 항상 전체 범위를 다시 계산하므로 통째로 갈아 끼운다 (멱등).
    write_jdbc(final, "daily_settlement", mode="overwrite", truncate=True)
    print(f"  {len(rows)}행 기록 완료 (truncate 후 전체 재적재 - 멱등)")

    # ---------------------------------------------------------------- 7)
    hr("7) 분 단위 확정 집계 (대시보드가 실시간과 겹쳐 그리는 용도)")
    # daily_settlement 는 정산용(일 단위)이고, 이건 관찰용이다.
    # 대시보드에서 Redis 실시간 선과 이 확정 선을 같은 축에 겹쳐야
    # "어느 분에서 갈라졌는지" 가 보인다.
    jdbc_execute(spark, """
        CREATE TABLE IF NOT EXISTS minute_settlement (
            minute_ts   TIMESTAMPTZ NOT NULL,
            campaign_id TEXT        NOT NULL,
            impressions BIGINT      NOT NULL DEFAULT 0,
            clicks      BIGINT      NOT NULL DEFAULT 0,
            completes   BIGINT      NOT NULL DEFAULT 0,
            requests    BIGINT      NOT NULL DEFAULT 0,
            computed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            PRIMARY KEY (minute_ts, campaign_id)
        )
    """)
    minute = F.date_trunc("minute", F.col("event_time"))
    m_imp = imp.groupBy(minute.alias("minute_ts"), "campaign_id")                .agg(F.count("*").alias("impressions"))
    m_clk = (base.filter(F.col("event_type") == "click")
                 .groupBy(minute.alias("minute_ts"), "campaign_id")
                 .agg(F.count("*").alias("clicks")))
    m_cmp = (base.filter((F.col("event_type") == "quartile") & (F.col("quartile") == "complete"))
                 .groupBy(minute.alias("minute_ts"), "campaign_id")
                 .agg(F.count("*").alias("completes")))
    m_req = (base.filter((F.col("event_type") == "ad_response") & (F.col("fill") == True))  # noqa: E712
                 .groupBy(minute.alias("minute_ts"), "campaign_id")
                 .agg(F.count("*").alias("requests")))
    minute_df = (m_imp.join(m_clk, ["minute_ts", "campaign_id"], "full_outer")
                      .join(m_cmp, ["minute_ts", "campaign_id"], "full_outer")
                      .join(m_req, ["minute_ts", "campaign_id"], "full_outer")
                      .fillna(0, ["impressions", "clicks", "completes", "requests"])
                      .withColumn("computed_at", F.current_timestamp()))
    write_jdbc(minute_df, "minute_settlement", mode="overwrite", truncate=True)
    print(f"  {minute_df.count()}행 기록 (분 x 캠페인)")

    hr("완료")
    print("  다음: bash scripts/recon.sh   (실시간 합계 vs 확정 합계 대사)")
    spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
