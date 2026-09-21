"""E2E — 이벤트를 넣고 파이프라인 전 구간에서 정확한 숫자로 확인한다.

scripts/e2e.sh 가 tracer 컨테이너 안에서 이 파일을 두 번 실행한다 (Kafka/Redis/Postgres/MinIO 클라이언트가
그 이미지에 다 있어서 호스트에 아무것도 설치할 필요가 없다).

    python - send  <run_id>          이벤트를 넣고 상태(JSON)를 stdout 마지막 줄로 출력
    python - check <state_b64>       배치·대사가 끝난 뒤 전 구간을 확인. 실패가 있으면 exit 1

실행마다 전용 캠페인(cmp-e2e-<run_id>)을 쓴다. 배경 부하(cmp-1001~1005)와 섞이지 않아야
"이 테스트가 넣은 것만" 정확히 셀 수 있다.

넣는 것과 기대값 (impression 기준)
  정상 5건                       실시간 +5   확정 +5   원본 5행
  중복 재전송 1쌍 (같은 event_id)  실시간 +1   확정 +1   원본 2행   <- Flink·Spark 둘 다 접는다
  SSAI 이중경로 1쌍               실시간 +2   확정 +1   원본 2행   <- Spark 만 접는다
  90초 지연 1건                   실시간 +0   확정 +1   원본 1행   <- 윈도우가 버리고 late_dropped 에 남는다
  ----------------------------------------------------------------
                                  실시간 8    확정 8    원본 10행, raw(event_id 기준) 9, SSAI 1, 지각 1, 잔차 0
  스키마 위반 1건 / 서명 위조 픽셀 1건  -> dlq.invalid 로만 간다
  광고 결정 1건                    -> event_outbox published=true, ad.request 에 ad_response
"""
import base64
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

import psycopg2
import redis
import requests
from confluent_kafka import Consumer, TopicPartition

COLLECTOR = os.getenv("COLLECTOR_URL", "http://collector:8080")
ADDECISION = os.getenv("AD_DECISION_URL", "http://ad-decision:8090")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
BUCKET = os.getenv("MINIO_BUCKET", "events")
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
PG = dict(host=os.getenv("PG_HOST", "postgres"), port=5432,
          dbname=os.getenv("PG_DB", "adplatform"), user=os.getenv("PG_USER", "ads"),
          password=os.getenv("PG_PASSWORD", "ads"))
TOPICS = ["ad.impression", "ad.request", "dlq.invalid"]


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def now_ms():
    return int(time.time() * 1000)


def pg():
    return psycopg2.connect(connect_timeout=5, **PG)


def consumer():
    return Consumer({"bootstrap.servers": KAFKA, "group.id": f"e2e-{uuid.uuid4().hex[:8]}",
                     "enable.auto.commit": False, "auto.offset.reset": "earliest"})


def end_offsets():
    c = consumer()
    out = {}
    md = c.list_topics(timeout=10)
    for t in TOPICS:
        for p in md.topics[t].partitions:
            out[f"{t}:{p}"] = c.get_watermark_offsets(TopicPartition(t, p), timeout=5)[1]
    c.close()
    return out


def event(campaign, eid, ts, arid, source="client"):
    return {"event_id": eid, "event_type": "impression", "campaign_id": campaign,
            "event_time": ts, "ad_request_id": arid, "creative_id": "crt-e2e",
            "session_id": "sess-e2e", "user_id": "user-e2e", "device": "smart_tv",
            "content_id": "ct-e2e", "source": source}


def post(events):
    r = requests.post(f"{COLLECTOR}/v1/events", json=events, timeout=10)
    return r.status_code


# =========================================================================== send
def send(run_id):
    camp = f"cmp-e2e-{run_id}"
    with pg() as conn, conn.cursor() as cur:
        # Flink lookup join / Spark 단가 조인이 찾을 행. 예산 0 이라 광고 결정에서는 안 뽑힌다.
        cur.execute("INSERT INTO campaigns(campaign_id, advertiser, name, vertical, daily_budget) "
                    "VALUES (%s,'E2E','E2E 테스트','test',0) ON CONFLICT DO NOTHING", (camp,))
        cur.execute("INSERT INTO campaign_rates(campaign_id, cpm, cpc) VALUES (%s, 10000, 0) "
                    "ON CONFLICT DO NOTHING", (camp,))
    offsets = end_offsets()
    ts = now_ms()
    new = lambda p: f"{p}-e2e-{uuid.uuid4().hex[:12]}"   # noqa: E731
    st = {"run_id": run_id, "campaign": camp, "offsets": offsets, "ts": ts, "http": {}}

    normal = [event(camp, new("evt"), ts, new("req")) for _ in range(5)]
    st["normal"] = [e["event_id"] for e in normal]
    st["normal_arids"] = [e["ad_request_id"] for e in normal]
    st["http"]["normal"] = post(normal)

    dup = event(camp, new("evt"), ts, new("req"))
    st["dup"] = dup["event_id"]
    st["http"]["dup"] = [post([dup]), (time.sleep(0.4), post([dict(dup)]))[1]]

    arid = new("req")
    a, b = event(camp, new("evt"), ts, arid, "client"), event(camp, new("evt"), ts, arid, "server")
    b["ssai_twin_of"] = a["event_id"]
    st["ssai"] = [a["event_id"], b["event_id"]]
    st["http"]["ssai"] = post([a, b])

    late = event(camp, new("evt"), ts - 90_000, new("req"))
    late["_late"] = True
    st["late"] = late["event_id"]
    st["late_ts"] = late["event_time"]
    st["http"]["late"] = post([late])

    bad = event(camp, new("evt"), ts, new("req"))
    bad.pop("campaign_id")
    st["bad"] = bad["event_id"]
    st["http"]["bad"] = post([bad])

    forged = new("evt")
    params = {"eid": forged, "et": "impression", "cid": camp, "ts": str(ts), "arid": new("req"),
              "sid": "sess-e2e", "src": "client", "exp": str(int(time.time()) + 300),
              "sig": "deadbeef" + uuid.uuid4().hex[:24]}
    r = requests.get(f"{COLLECTOR}/v1/track?" + urlencode(params), timeout=10)
    st["forged"] = forged
    st["http"]["forged"] = [r.status_code, r.headers.get("Content-Type", "")]

    dec_arid = new("req")
    r = requests.post(f"{ADDECISION}/v1/ad-request", timeout=10, json={
        "ad_request_id": dec_arid, "session_id": "sess-e2e", "user_id": "user-e2e",
        "content_id": "ct-e2e", "device": "smart_tv", "ad_pod_id": "pod-e2e",
        "ad_slot": 0, "playhead_s": 0, "max_duration_s": 30})
    st["decision"] = {"arid": dec_arid, "status": r.status_code, "body": r.json()}
    log(f"[e2e] 캠페인 {camp} 에 이벤트를 넣었다")
    print(json.dumps(st))


# =========================================================================== check
class Checks:
    def __init__(self):
        self.results = []

    def eq(self, name, got, want):
        self.results.append((got == want, name, f"{got!r}" if got == want else f"{got!r} (기대 {want!r})"))

    def ok(self, name, cond, detail=""):
        self.results.append((bool(cond), name, detail))

    def report(self):
        width = max(len(n) for _, n, _ in self.results)
        for passed, name, detail in self.results:
            print(f"  {'PASS' if passed else 'FAIL'}  {name:<{width}}  {detail}")
        failed = sum(1 for p, _, _ in self.results if not p)
        print(f"\n  {len(self.results) - failed}/{len(self.results)} 통과")
        return failed


def scan_kafka(offsets):
    """send 직전 오프셋부터 지금 끝까지 읽는다. {topic: [(partition, value_str)]}"""
    c = consumer()
    tps, ends = [], {}
    for k, start in offsets.items():
        t, p = k.rsplit(":", 1)
        hi = c.get_watermark_offsets(TopicPartition(t, int(p)), timeout=5)[1]
        if hi > start:
            tps.append(TopicPartition(t, int(p), start))
            ends[(t, int(p))] = hi
    out = {t: [] for t in TOPICS}
    if tps:
        c.assign(tps)
        left = set(ends)
        deadline = time.time() + 60
        while left and time.time() < deadline:
            m = c.poll(1.0)
            if m is None or m.error():
                continue
            out[m.topic()].append((m.partition(), m.value().decode("utf-8", "replace")))
            if m.offset() >= ends[(m.topic(), m.partition())] - 1:
                left.discard((m.topic(), m.partition()))
    c.close()
    return out


def check(st):
    ck = Checks()
    camp = st["campaign"]

    # ---- 1. Collector
    h = st["http"]
    ck.eq("collector: 정상 배치 응답", h["normal"], 202)
    ck.eq("collector: 중복 재전송 응답 (두 번 다)", h["dup"], [202, 202])
    ck.eq("collector: 스키마 위반도 202 (fail-open, 거절하지 않는다)", h["bad"], 202)
    ck.ok("collector: 위조 픽셀도 200 + GIF", h["forged"][0] == 200 and "gif" in h["forged"][1],
          str(h["forged"]))
    ck.eq("ad-decision: 응답", st["decision"]["status"], 200)

    # ---- 2. Kafka
    k = scan_kafka(st["offsets"])
    imp = [(p, json.loads(v)) for p, v in k["ad.impression"]]
    imp_ids = [e.get("event_id") for _, e in imp]
    ck.eq("kafka: 정상 5건이 각 1번씩", sorted(imp_ids.count(i) for i in st["normal"]), [1] * 5)
    ck.eq("kafka: 중복 재전송은 2번 (Kafka 는 거르지 않는다)", imp_ids.count(st["dup"]), 2)
    ck.eq("kafka: SSAI 쌍 둘 다", [imp_ids.count(i) for i in st["ssai"]], [1, 1])
    ck.eq("kafka: 지연 이벤트 1번", imp_ids.count(st["late"]), 1)
    ck.eq("kafka: 스키마 위반은 ad.impression 에 없다", imp_ids.count(st["bad"]), 0)
    ck.eq("kafka: 위조 픽셀은 ad.impression 에 없다", imp_ids.count(st["forged"]), 0)
    dlq = [v for _, v in k["dlq.invalid"]]
    ck.ok("kafka: 스키마 위반은 dlq.invalid 로", any(st["bad"] in v for v in dlq))
    ck.ok("kafka: 위조 픽셀은 dlq.invalid 로", any(st["forged"] in v for v in dlq))
    ssai_parts = {p for p, e in imp if e.get("event_id") in st["ssai"]}
    ck.eq("kafka: 같은 ad_request_id 는 같은 파티션 (SSAI 쌍)", len(ssai_parts), 1)
    resp = [json.loads(v) for _, v in k["ad.request"]]
    ck.ok("kafka: ad.request 에 Outbox 경유 ad_response",
          any(e.get("ad_request_id") == st["decision"]["arid"] and e.get("event_type") == "ad_response"
              and e.get("transport") == "outbox" for e in resp))

    # ---- 3. Redis (Flink 실시간)
    r = redis.Redis(host=REDIS_HOST, decode_responses=True)
    keys = r.keys(f"agg:1m:{camp}:*")          # 테스트 전용 캠페인이라 키가 몇 개뿐이다
    rt_imp = sum(int(float(r.hget(k2, "impressions") or 0)) for k2 in keys)
    rt_ssai = sum(int(float(r.hget(k2, "ssai_dupes") or 0)) for k2 in keys)
    ck.eq("redis: 실시간 impression = 정상5 + 중복1 + SSAI2 (+지연0)", rt_imp, 8)
    ck.eq("redis: ssai_dupes", rt_ssai, 1)

    # ---- 4. Postgres: late_dropped, outbox
    with pg() as conn, conn.cursor() as cur:
        cur.execute("SELECT event_id, kind FROM late_dropped WHERE campaign_id = %s", (camp,))
        late_rows = cur.fetchall()
        ck.eq("late_dropped: 지연 이벤트 1건만", late_rows, [(st["late"], "impression")])
        cur.execute("SELECT count(*), bool_and(published) FROM event_outbox WHERE aggregate_id = %s",
                    (st["decision"]["arid"],))
        n, published = cur.fetchone()
        ck.eq("outbox: 결정 1건이 발행 완료", (n, published), (1, True))

        # ---- 5. 배치 (MinIO 원본 -> Spark -> minute_settlement)
        cur.execute("SELECT coalesce(sum(impressions),0), coalesce(sum(raw_impressions),0) "
                    "FROM minute_settlement WHERE campaign_id = %s", (camp,))
        b_imp, b_raw = (int(x) for x in cur.fetchone())
        ck.eq("batch: 확정 impression = 정상5 + 중복1 + SSAI1 + 지연1", b_imp, 8)
        ck.eq("batch: raw(event_id 기준, SSAI 정리 전)", b_raw, 9)

        # ---- 6. 대사
        cur.execute("SELECT campaign_id, realtime_impressions, batch_impressions, likely_cause "
                    "FROM reconciliation WHERE run_at = (SELECT max(run_at) FROM reconciliation)")
        rec = {row[0]: row[1:] for row in cur.fetchall()}
    mine = rec.get(camp)
    ck.ok("recon: 테스트 캠페인 행이 있다", mine is not None)
    if mine:
        ck.eq("recon: 실시간 / 확정", (mine[0], mine[1]), (8, 8))
        ck.ok("recon: 원인 = 지연반영 +1, SSAI -1, 잔차 없음",
              "지연반영 +1" in mine[2] and "SSAI이중경로 -1" in mine[2] and "잔차" not in mine[2], mine[2])
    residual = {c: v[2] for c, v in rec.items() if "잔차" in (v[2] or "") or "미집계" in (v[2] or "")
                or "미적재" in (v[2] or "")}
    ck.ok(f"recon: 전체 {len(rec)}개 캠페인 모두 잔차 0 (배경 부하 포함)", not residual,
          "; ".join(f"{c}: {v}" for c, v in residual.items())[:300])

    # ---- 7. MinIO 원본
    try:
        import pyarrow.dataset as pads
        from pyarrow import fs as pafs
        s3 = pafs.S3FileSystem(endpoint_override=MINIO_HOST, access_key=MINIO_KEY,
                               secret_key=MINIO_SECRET, scheme="http", region="us-east-1")
        hours = {datetime.fromtimestamp(t / 1000, timezone.utc).strftime("dt=%Y-%m-%d/hour=%H")
                 for t in (st["late_ts"], st["ts"])}
        rows = 0
        for hp in sorted(hours):
            ds = pads.dataset(f"{BUCKET}/{hp}", filesystem=s3, format="parquet")
            rows += ds.count_rows(filter=pads.field("campaign_id") == camp)
        ck.eq("minio: 원본 Parquet 행 = 정상5 + 중복2 + SSAI2 + 지연1 (가공 없음)", rows, 10)
    except Exception as e:
        ck.ok("minio: 원본 Parquet 조회", False, str(e)[:200])

    return ck.report()


if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else ""
    if mode == "send":
        send(sys.argv[2])
    elif mode == "check":
        # 상태는 base64 로 받는다 (JSON 을 명령줄로 넘기면 Windows 셸에서 따옴표가 깨진다)
        sys.exit(1 if check(json.loads(base64.b64decode(sys.argv[2]))) else 0)
    else:
        sys.exit("사용법: pipeline_checks.py send <run_id> | check <state_json>")
