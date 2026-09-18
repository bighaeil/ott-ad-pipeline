"""이벤트 추적기 — 클릭 한 번으로 이벤트를 넣고 파이프라인을 따라간다.

버튼을 누르면 이벤트를 만들어 Collector 로 보내고, 그 event_id 가
아래 다섯 자리에 언제 나타나는지를 1초마다 확인해 보여 준다.

  1. Collector      HTTP 응답 (accepted / invalid / fallback)
  2. Kafka          어느 토픽 / 파티션 / 오프셋에 앉았는지
  3. Flink -> Redis 분단위 집계에 몇 건으로 반영됐는지
  4. MinIO          Parquet 원본에 몇 행으로 적재됐는지
  5. PostgreSQL     확정 집계 (배치를 돌려야 나온다)

추적 대상은 전용 캠페인 cmp-trace 를 쓴다.
생성기 트래픽(cmp-1001~1005)과 섞이지 않아야 "내가 누른 것만" 셀 수 있기 때문이다.

Kafka 스캔은 전송 직전의 오프셋을 기억해 두고 그 지점부터만 훑는다.
안 그러면 부하가 도는 중에 토픽 전체를 매번 읽어야 한다.
"""
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from urllib.parse import urlencode

import psycopg2
import redis
import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

COLLECTOR = os.getenv("COLLECTOR_URL", "http://collector:8080")
ADDECISION = os.getenv("AD_DECISION_URL", "http://ad-decision:8090")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
SECRET = os.getenv("COLLECTOR_HMAC_SECRET", "local-dev-secret").encode()
BUCKET = os.getenv("MINIO_BUCKET", "events")
MINIO_HOST = os.getenv("MINIO_HOST", "minio:9000")
MINIO_KEY = os.getenv("MINIO_ROOT_USER", "minioadmin")
MINIO_SECRET = os.getenv("MINIO_ROOT_PASSWORD", "minioadmin")
PG = dict(host=os.getenv("PG_HOST", "postgres"), port=5432,
          dbname=os.getenv("PG_DB", "adplatform"), user=os.getenv("PG_USER", "ads"),
          password=os.getenv("PG_PASSWORD", "ads"))

# 추적마다 캠페인을 새로 판다.
# 하나를 공유하면 같은 분 윈도우에 두 번 누를 때 서로의 건수를 같이 세어
# "중복은 +1, SSAI 는 +2" 같은 비교가 성립하지 않는다.
TRACE_CAMPAIGN_PREFIX = "cmp-tr-"
WATCH_TOPICS = ["ad.impression", "ad.quartile", "ad.click", "ad.request",
                "user.behavior", "dlq.invalid", "late.events"]

app = FastAPI(title="ott-ads event tracer")
HERE = os.path.dirname(os.path.abspath(__file__))

TRACES = {}
_lock = threading.Lock()
_kafka_lock = threading.Lock()

_rds = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True,
                   socket_connect_timeout=3, socket_timeout=3)
_admin = AdminClient({"bootstrap.servers": KAFKA})
_consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "tracer-scan",
                      "enable.auto.commit": False, "auto.offset.reset": "earliest"})


# ============================================================== 이벤트 정의
# 각 버튼이 무엇을 만들고, 각 단계에서 무엇을 기대하는지 한곳에 모았다.
KINDS = {
    "normal": {
        "title": "정상 임프레션 1건",
        "desc": "가장 단순한 경로. 다섯 단계를 모두 통과한다.",
        "expect": {"kafka": "ad.impression 1건", "redis": "+1", "minio": "1행", "pg": "1"},
        "lesson": "이게 기준선이다. 아래 버튼들은 전부 이 흐름에서 무언가가 어긋난 경우다.",
    },
    "duplicate": {
        "title": "중복 — 같은 event_id 2번",
        "desc": "플레이어가 같은 이벤트를 재전송한 상황.",
        "expect": {"kafka": "ad.impression 2건", "redis": "+1 (접힘)", "minio": "2행", "pg": "1"},
        "lesson": "Kafka 에는 2건이 그대로 앉지만 Flink 가 event_id 로 접어 Redis 는 +1 이다. "
                  "Parquet 은 원본이라 2행이고, Spark 가 다시 접어 확정은 1이다.",
    },
    "ssai": {
        "title": "SSAI 이중경로 — event_id 다름",
        "desc": "같은 광고(ad_request_id)가 클라이언트/서버 두 경로로 도착.",
        "expect": {"kafka": "ad.impression 2건", "redis": "+2 (못 접음!)", "minio": "2행", "pg": "1"},
        "lesson": "위 '중복' 과 비교할 것. event_id 가 다르므로 Flink 는 둘 다 센다. "
                  "실시간 집계가 부풀어 있는 상태이고, ad_request_id 로 묶는 Spark 배치만 이걸 정리한다.",
    },
    "late": {
        "title": "지연 이벤트 — 60초 과거",
        "desc": "네트워크 복구 후 밀린 전송. event_time 이 60초 전이다.",
        "expect": {"kafka": "ad.impression 1건 + late.events", "redis": "+0 (윈도우 놓침)",
                   "minio": "1행", "pg": "1"},
        "lesson": "워터마크(10초)를 한참 지나 도착해 실시간 윈도우가 버린다. "
                  "late.events 토픽에는 남고, event_time 만 보는 배치는 정상 집계한다. "
                  "실시간 < 확정 이 되는 이유가 이것이다.",
    },
    "bad_schema": {
        "title": "스키마 위반 — campaign_id 누락",
        "desc": "필수 필드가 빠진 이벤트.",
        "expect": {"kafka": "dlq.invalid", "redis": "+0", "minio": "0행", "pg": "0"},
        "lesson": "Collector 가 검증에서 걸러 dlq.invalid 로 보낸다. 버리지 않고 사유와 원본을 남긴다. "
                  "응답은 202 다 — 배치 전체를 거절하면 플레이어가 재전송 폭주를 일으키기 때문.",
    },
    "forged": {
        "title": "서명 위조 트래킹 픽셀",
        "desc": "sig 가 틀린 /v1/track 호출. 임프레션 부풀리기 시도.",
        "expect": {"kafka": "dlq.invalid", "redis": "+0", "minio": "0행", "pg": "0"},
        "lesson": "HTTP 는 200 + 1x1 GIF 로 답한다 (4xx 를 주면 플레이어가 재시도 폭주). "
                  "대신 집계에는 한 건도 안 들어간다. 서명 검증이 정산의 방어선이다.",
    },
    "ad_request": {
        "title": "광고 요청 — Outbox 경로",
        "desc": "ad-decision 이 소재를 정하고 같은 트랜잭션에서 event_outbox 에 INSERT.",
        "expect": {"kafka": "ad.request (Outbox 워커 경유)", "redis": "requests +1",
                   "minio": "1행", "pg": "requests"},
        "lesson": "이건 Collector 를 거치지 않는다. DB 에 먼저 쓰고 워커가 Kafka 로 옮긴다. "
                  "그래서 Kafka 도착이 0.5초쯤 늦다 (워커 폴링 주기).",
    },
}


# ================================================================= 유틸
def now_ms():
    return int(time.time() * 1000)


def minute_key(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y%m%d%H%M")


def sign(params):
    canonical = "&".join(f"{k}={params[k]}" for k in sorted(params) if k != "sig")
    return hmac.new(SECRET, canonical.encode(), hashlib.sha256).hexdigest()


def end_offsets():
    """지금 시점의 토픽별 끝 오프셋. 여기서부터만 스캔한다."""
    out = {}
    try:
        md = _admin.list_topics(timeout=5)
    except Exception:
        return out
    with _kafka_lock:
        for t in WATCH_TOPICS:
            tm = md.topics.get(t)
            if not tm:
                continue
            for p in tm.partitions:
                try:
                    _lo, hi = _consumer.get_watermark_offsets(
                        TopicPartition(t, p), timeout=2, cached=False)
                    out[f"{t}:{p}"] = hi
                except Exception:
                    pass
    return out


def ensure_campaign(campaign_id):
    """추적 전용 캠페인을 DB 에 만들어 둔다.

    Flink 의 lookup join 과 Spark 의 단가 조인이 이 행을 찾는다.
    이벤트를 보내기 전에 넣어야 lookup 캐시에 miss 가 박히지 않는다.
    """
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
        with conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO campaigns(campaign_id, advertiser, name, vertical, daily_budget) "
                "VALUES (%s,'추적용','이벤트 추적기','trace',0) ON CONFLICT DO NOTHING",
                (campaign_id,))
            cur.execute(
                "INSERT INTO campaign_rates(campaign_id, cpm, cpc) VALUES (%s, 10000, 0) "
                "ON CONFLICT DO NOTHING", (campaign_id,))
        conn.close()
        return True
    except Exception as e:
        print(f"[tracer] {campaign_id} 캠페인 준비 실패: {e}", flush=True)
        return False


# =============================================================== 이벤트 발사
def base_event(campaign, event_id, ts, arid, source="client", event_type="impression"):
    return {
        "event_id": event_id, "event_type": event_type,
        "campaign_id": campaign, "event_time": ts,
        "ad_request_id": arid, "creative_id": "crt-trace-a",
        "session_id": "sess-trace", "user_id": "user-trace",
        "device": "smart_tv", "content_id": "ct-trace", "source": source,
    }


def send(kind, campaign):
    ts = now_ms()
    arid = "req-trace-" + uuid.uuid4().hex[:10]
    ids, http = [], []

    if kind == "normal":
        e = base_event(campaign, "evt-trace-" + uuid.uuid4().hex[:12], ts, arid)
        ids.append({"event_id": e["event_id"], "role": "임프레션"})
        http.append(post_events([e]))

    elif kind == "duplicate":
        eid = "evt-trace-" + uuid.uuid4().hex[:12]
        e = base_event(campaign, eid, ts, arid)
        ids.append({"event_id": eid, "role": "원본 + 재전송 (같은 event_id)"})
        http.append(post_events([e]))
        time.sleep(0.4)
        http.append(post_events([dict(e)]))          # 같은 event_id 를 한 번 더

    elif kind == "ssai":
        a = "evt-trace-" + uuid.uuid4().hex[:12]
        b = "evt-trace-" + uuid.uuid4().hex[:12]
        ev_a = base_event(campaign, a, ts, arid, "client")
        ev_b = base_event(campaign, b, ts, arid, "server")
        ev_b["ssai_twin_of"] = a
        ids += [{"event_id": a, "role": "클라이언트 경로"},
                {"event_id": b, "role": "서버 경로 (SSAI 스티처)"}]
        http.append(post_events([ev_a, ev_b]))

    elif kind == "late":
        eid = "evt-trace-" + uuid.uuid4().hex[:12]
        e = base_event(campaign, eid, ts - 60_000, arid)   # 60초 과거
        e["_late"] = True
        ids.append({"event_id": eid, "role": "60초 지연 임프레션"})
        http.append(post_events([e]))

    elif kind == "bad_schema":
        eid = "evt-trace-" + uuid.uuid4().hex[:12]
        e = base_event(campaign, eid, ts, arid)
        e.pop("campaign_id")                          # 필수 필드 제거
        ids.append({"event_id": eid, "role": "campaign_id 없는 임프레션"})
        http.append(post_events([e]))

    elif kind == "forged":
        eid = "evt-trace-" + uuid.uuid4().hex[:12]
        params = {"eid": eid, "et": "impression", "cid": campaign, "ts": str(ts),
                  "arid": arid, "sid": "sess-trace", "src": "client",
                  "exp": str(int(time.time()) + 300)}
        params["sig"] = "deadbeef" + uuid.uuid4().hex[:24]   # 일부러 틀린 서명
        ids.append({"event_id": eid, "role": "서명 위조 픽셀"})
        try:
            r = requests.get(f"{COLLECTOR}/v1/track?" + urlencode(params), timeout=5)
            http.append({"status": r.status_code, "body": f"{r.headers.get('Content-Type')} "
                                                          f"{len(r.content)}바이트 (1x1 GIF)"})
        except Exception as e:
            http.append({"status": 0, "body": str(e)})

    elif kind == "ad_request":
        try:
            r = requests.post(f"{ADDECISION}/v1/ad-request", timeout=5, json={
                "ad_request_id": arid, "session_id": "sess-trace", "user_id": "user-trace",
                "content_id": "ct-trace", "device": "smart_tv", "ad_pod_id": "pod-trace",
                "ad_slot": 0, "playhead_s": 0, "max_duration_s": 30})
            body = r.json()
            http.append({"status": r.status_code, "body": json.dumps(body, ensure_ascii=False)})
            ids.append({"event_id": arid, "role": "ad_request_id (Outbox 로 나간다)",
                        "match": "ad_request_id"})
        except Exception as e:
            http.append({"status": 0, "body": str(e)})

    return ts, arid, ids, http


NUDGE_CAMPAIGN = "cmp-9999"


def nudge_watermark(trace_id):
    """워터마크를 앞으로 밀어 추적 대상의 윈도우를 닫는다.

    Flink 이벤트타임 윈도우는 "워터마크가 윈도우 끝을 지날 때" 발화하고,
    워터마크는 들어오는 이벤트의 event_time 에서 나온다.
    추적기만 쓰는 상황(생성기 부하 없음)에서는 내가 넣은 1건 뒤로 아무것도 안 와서
    윈도우가 영원히 안 닫힌다 -> Redis 단계가 끝내 '대기' 로 남는다.

    그래서 30초/65초 뒤에 현재 시각 이벤트를 소량 흘려 워터마크를 민다.
    추적 대상과 섞이지 않게 별도 캠페인(cmp-9999)을 쓴다.
    운영에서는 트래픽이 끊기지 않으므로 이런 장치가 필요 없다.
    """
    for delay in (30, 65):
        time.sleep(delay if delay == 30 else 35)
        if trace_id not in TRACES:
            return
        ts = now_ms()
        tag = uuid.uuid4().hex[:8]
        post_events([{
            "event_id": f"evt-nudge-{tag}", "event_type": "impression",
            "campaign_id": NUDGE_CAMPAIGN, "event_time": ts,
            "ad_request_id": f"req-nudge-{tag}", "source": "client",
        }])
        with _lock:
            tr = TRACES.get(trace_id)
            if tr is not None:
                tr.setdefault("nudges", []).append(round(time.time() - tr["t0"], 1))


def post_events(events):
    try:
        r = requests.post(f"{COLLECTOR}/v1/events", json=events, timeout=8)
        return {"status": r.status_code, "body": r.text}
    except Exception as e:
        return {"status": 0, "body": str(e)}


# ================================================================ 단계 확인
def scan_kafka(tr):
    """기억해 둔 오프셋부터 현재 끝까지 훑어 우리 이벤트를 찾는다."""
    targets = {i["event_id"] for i in tr["ids"]}
    match_field = tr["ids"][0].get("match", "event_id") if tr["ids"] else "event_id"
    try:
        md = _admin.list_topics(timeout=5)
    except Exception:
        return

    with _kafka_lock:
        tps = []
        for key, off in tr["offsets"].items():
            t, p = key.rsplit(":", 1)
            if t not in md.topics:
                continue
            tps.append(TopicPartition(t, int(p), off))
        if not tps:
            return
        try:
            _consumer.assign(tps)
        except Exception:
            return
        deadline = time.time() + 2.0
        while time.time() < deadline:
            msg = _consumer.poll(0.2)
            if msg is None:
                continue
            if msg.error():
                continue
            key = f"{msg.topic()}:{msg.partition()}"
            tr["offsets"][key] = msg.offset() + 1
            try:
                d = json.loads(msg.value())
            except Exception:
                continue
            # dlq.invalid 는 원본이 raw 안에 들어 있다
            cand = {d.get(match_field), d.get("event_id"), d.get("ad_request_id")}
            raw = d.get("raw") or {}
            if isinstance(raw, dict):
                cand |= {raw.get("event_id"), raw.get("eid"), raw.get("ad_request_id")}
            if cand & targets:
                tr["kafka"].append({
                    "topic": msg.topic(), "partition": msg.partition(),
                    "offset": msg.offset(), "at": round(time.time() - tr["t0"], 2),
                    "reason": d.get("reason"),
                    "lateness_ms": d.get("lateness_ms"),
                    "source": d.get("source"),
                })
        try:
            _consumer.unassign()
        except Exception:
            pass


def check_redis(tr):
    key = f"agg:1m:{tr['campaign']}:{tr['minute']}"
    try:
        h = _rds.hgetall(key)
    except Exception:
        return None
    if not h:
        return None
    return {"key": key,
            "impressions": int(float(h.get("impressions") or 0)),
            "requests": int(float(h.get("requests") or 0)),
            "ssai_dupes": int(float(h.get("ssai_dupes") or 0)),
            "at": round(time.time() - tr["t0"], 1)}


_PARQUET_CACHE = {}


def check_minio(tr):
    """Parquet 안에 우리 event_id 가 몇 행 있는지."""
    ck = tr["trace_id"]
    hit = _PARQUET_CACHE.get(ck)
    if hit and time.time() - hit[0] < 5:
        return hit[1]
    try:
        import pyarrow.dataset as pads
        from pyarrow import fs as pafs
        s3 = pafs.S3FileSystem(endpoint_override=MINIO_HOST, access_key=MINIO_KEY,
                               secret_key=MINIO_SECRET, scheme="http", region="us-east-1")
        path = f"{BUCKET}/dt={tr['dt']}/hour={tr['hour']}"
        ds = pads.dataset(path, filesystem=s3, format="parquet")
        ids = [i["event_id"] for i in tr["ids"]]
        field = tr["ids"][0].get("match", "event_id") if tr["ids"] else "event_id"
        tbl = ds.to_table(columns=["event_id", "ad_request_id", "event_type", "source"],
                          filter=pads.field(field).isin(ids))
        res = {"rows": tbl.num_rows, "path": f"s3a://{path}/",
               "at": round(time.time() - tr["t0"], 1)} if tbl.num_rows else None
    except Exception as e:
        res = None
        tr["minio_err"] = str(e)[:160]
    _PARQUET_CACHE[ck] = (time.time(), res)
    return res


def check_pg(tr):
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT impressions, requests, raw_impressions, amount "
                        "FROM daily_settlement WHERE campaign_id=%s AND dt=%s",
                        (tr["campaign"], tr["dt"]))
            r = cur.fetchone()
        conn.close()
        if not r:
            return None
        return {"impressions": r[0], "requests": r[1], "raw_impressions": r[2],
                "amount": float(r[3] or 0)}
    except Exception:
        return None


# ==================================================================== API
class SendReq(BaseModel):
    kind: str


@app.on_event("startup")
def startup():
    print("[tracer] ready on :3000", flush=True)


@app.get("/api/kinds")
def kinds():
    return {"kinds": [{"id": k, **v} for k, v in KINDS.items()]}


@app.post("/api/send")
def api_send(req: SendReq):
    if req.kind not in KINDS:
        return JSONResponse({"error": "unknown kind"}, 400)
    trace_id = uuid.uuid4().hex[:10]
    campaign = TRACE_CAMPAIGN_PREFIX + trace_id
    ensure_campaign(campaign)
    offsets = end_offsets()
    t0 = time.time()
    ts, arid, ids, http = send(req.kind, campaign)
    now = datetime.fromtimestamp(ts / 1000, timezone.utc)
    tr = {
        "trace_id": trace_id, "campaign": campaign,
        "kind": req.kind, "t0": t0, "ts": ts, "arid": arid, "ids": ids, "http": http,
        "offsets": offsets, "kafka": [],
        "minute": minute_key(ts), "dt": now.strftime("%Y-%m-%d"), "hour": now.strftime("%H"),
        "redis_base": None,
    }
    tr["redis_base"] = check_redis(tr)
    with _lock:
        TRACES[tr["trace_id"]] = tr
        for old in list(TRACES)[:-20]:
            TRACES.pop(old, None)
    threading.Thread(target=nudge_watermark, args=(tr["trace_id"],), daemon=True).start()
    return {"trace_id": tr["trace_id"], "kind": req.kind, "ids": ids,
            "campaign_id": campaign,
            "ad_request_id": arid, "minute": tr["minute"], "http": http,
            "meta": KINDS[req.kind]}


@app.get("/api/trace/{trace_id}")
def api_trace(trace_id: str):
    tr = TRACES.get(trace_id)
    if not tr:
        return JSONResponse({"error": "not found"}, 404)
    scan_kafka(tr)
    redis_now = check_redis(tr)
    base = (tr["redis_base"] or {}).get("impressions", 0)
    delta = None
    if redis_now:
        delta = {
            "impressions": redis_now["impressions"] - base,
            "requests": redis_now["requests"] - ((tr["redis_base"] or {}).get("requests", 0)),
            "ssai_dupes": redis_now["ssai_dupes"],
            "at": redis_now["at"], "key": redis_now["key"],
        }
    return {
        "trace_id": trace_id, "kind": tr["kind"], "meta": KINDS[tr["kind"]],
        "elapsed": round(time.time() - tr["t0"], 1),
        "ids": tr["ids"], "ad_request_id": tr["arid"], "minute": tr["minute"],
        "campaign_id": tr["campaign"],
        "http": tr["http"],
        "kafka": tr["kafka"],
        "redis": delta,
        "minio": check_minio(tr),
        "minio_err": tr.get("minio_err"),
        "pg": check_pg(tr),
        "nudges": tr.get("nudges", []),
    }


@app.get("/api/health")
def health():
    return {"status": "UP"}


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "index.html"))
