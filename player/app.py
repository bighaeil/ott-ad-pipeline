"""OTT 플레이어 (데모 화면).

실제 OTT 앱의 재생 화면에 준하는 UI 를 띄우고, 거기서 일어나는 일을
"진짜 파이프라인" 에 그대로 흘린다. 화면에 물리적인 동영상 파일은 없다.
재생 중인 것처럼 보이는 것은 CSS 로 그린 화면과 재생위치 타이머뿐이고,
그 밖의 모든 것 — 광고 결정 호출, 비콘, 토픽 적재 — 은 실제 동작이다.

  1. 콘텐츠를 고르면          user.behavior 의 session_start / progress
  2. 광고 브레이크에 닿으면   ad-decision 의 POST /v1/ad-request  (-> Outbox -> ad.request)
  3. 광고가 재생되면          ad.impression / ad.quartile / ad.click
  4. "데이터 확인" 을 누르면  Kafka -> Redis -> MinIO -> PostgreSQL 어디까지 갔는지 조회

콘텐츠는 빨리 감아서(기본 60배) 광고 브레이크에 금방 닿게 하고,
광고 구간만 실제 시간으로 재생한다. 이벤트 간격이 실제와 같아야
Flink 윈도우/워터마크가 의미 있는 값을 만들기 때문이다.
"""
import hashlib
import hmac
import json
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import urlencode

import psycopg2
import redis
import requests
from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from catalog import CONTENTS, style_for

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

# 이벤트가 앉을 토픽. collector 의 TopicRouter.kt 와 같은 규칙을 복사해 둔 것이다.
# 화면에서 "이 이벤트는 어느 토픽으로 간다" 를 미리 보여 주는 용도이고,
# 실제 라우팅은 Collector 가 한다 (여기가 틀려도 적재에는 영향이 없다).
TOPIC_OF = {
    "impression": "ad.impression", "ad_impression": "ad.impression",
    "quartile": "ad.quartile", "start": "ad.quartile", "first_quartile": "ad.quartile",
    "midpoint": "ad.quartile", "third_quartile": "ad.quartile", "complete": "ad.quartile",
    "click": "ad.click", "ad_click": "ad.click",
    "ad_request": "ad.request", "ad_response": "ad.request",
    "progress": "user.behavior", "play": "user.behavior", "pause": "user.behavior",
    "resume": "user.behavior", "seek": "user.behavior",
    "session_start": "user.behavior", "session_end": "user.behavior",
    "content_start": "user.behavior", "content_end": "user.behavior",
}
WATCH_TOPICS = ["ad.impression", "ad.quartile", "ad.click", "ad.request",
                "user.behavior", "dlq.invalid", "late.events"]

NUDGE_CAMPAIGN = "cmp-9999"      # 워터마크 밀기 전용. 추적 대상과 섞이지 않게 분리.

app = FastAPI(title="ott-ads player")
HERE = os.path.dirname(os.path.abspath(__file__))

# ad_request_id -> 그 광고 1편의 추적 상태
PLAYS = {}
_lock = threading.Lock()
_kafka_lock = threading.Lock()

_rds = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True,
                   socket_connect_timeout=3, socket_timeout=3)
_admin = AdminClient({"bootstrap.servers": KAFKA})
_consumer = Consumer({"bootstrap.servers": KAFKA, "group.id": "player-scan",
                      "enable.auto.commit": False, "auto.offset.reset": "earliest"})
_http = requests.Session()

# Kafka 끝 오프셋 스냅샷. 광고 요청마다 조회하면 광고 시작이 느려지므로
# 백그라운드에서 3초마다 갱신해 두고 그 복사본을 쓴다.
# 최대 3초 앞에서부터 스캔하게 되지만, event_id / ad_request_id 로 걸러 내므로 결과는 같다.
_offsets_snapshot = {}


def now_ms():
    return int(time.time() * 1000)


def minute_key(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, timezone.utc).strftime("%Y%m%d%H%M")


def sign(params):
    """Collector 의 SignatureVerifier 와 같은 규칙."""
    canonical = "&".join(f"{k}={params[k]}" for k in sorted(params) if k != "sig")
    return hmac.new(SECRET, canonical.encode(), hashlib.sha256).hexdigest()


def refresh_offsets():
    global _offsets_snapshot
    while True:
        out = {}
        try:
            md = _admin.list_topics(timeout=5)
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
            if out:
                _offsets_snapshot = out
        except Exception:
            pass
        time.sleep(3)


# ============================================================ 이벤트 전송
def post_events(events):
    try:
        r = _http.post(f"{COLLECTOR}/v1/events", json=events, timeout=8)
        return {"status": r.status_code, "body": r.text[:200]}
    except Exception as e:
        return {"status": 0, "body": str(e)[:200]}


PIXEL_MAP = {"eid": "event_id", "et": "event_type", "cid": "campaign_id", "ts": "event_time",
             "arid": "ad_request_id", "crid": "creative_id", "q": "quartile",
             "sid": "session_id", "src": "source"}


def send_pixel(ev, forge=False):
    """VAST 트래킹 픽셀 경로.

    브라우저가 직접 쏘게 하려면 서명 비밀키를 화면에 내려 줘야 하고(그러면 위조가 자유로워진다)
    Collector 에 CORS 도 열어야 해서, 서버가 대신 호출한다.
    실제 플레이어에서는 광고 서버가 서명된 URL 을 VAST 응답에 넣어 주고
    플레이어는 그 URL 을 그대로 호출한다 — 서명은 어느 쪽이든 서버가 만든다.
    """
    params = {}
    for pk, ek in PIXEL_MAP.items():
        v = ev.get(ek)
        if v is not None:
            params[pk] = str(v)
    params["exp"] = str(int(time.time()) + 300)
    params["sig"] = uuid.uuid4().hex[:32] if forge else sign(params)
    try:
        r = _http.get(f"{COLLECTOR}/v1/track?" + urlencode(params), timeout=8)
        return {"status": r.status_code,
                "body": f"{r.headers.get('Content-Type')} {len(r.content)}B (1x1 GIF)"}
    except Exception as e:
        return {"status": 0, "body": str(e)[:200]}


def remember(ev):
    """광고 1편(ad_request_id)에 붙은 이벤트를 추적 상태에 기록한다."""
    arid = ev.get("ad_request_id")
    if not arid:
        return
    with _lock:
        tr = PLAYS.get(arid)
        if tr is None:
            return
        tr["event_ids"].append({"event_id": ev.get("event_id"),
                                "type": ev.get("event_type"),
                                "quartile": ev.get("quartile"),
                                "source": ev.get("source", "client")})


# =================================================================== 모델
class AdReq(BaseModel):
    # 광고 1편의 식별자. 플레이어가 먼저 만들어 ad_request 이벤트에 실어 보내고,
    # 같은 값을 여기로 넘긴다. 그래야 클라이언트가 본 사실(ad_request)과
    # 서버가 확정한 사실(ad_response)이 같은 키로 묶인다.
    ad_request_id: Optional[str] = None
    session_id: str
    user_id: str
    content_id: str
    device: str = "smart_tv"
    ad_pod_id: str
    ad_slot: int = 0
    playhead_s: float = 0.0
    max_duration_s: int = 30
    prefer_campaign_id: Optional[str] = None
    force_no_fill: bool = False


class EventBatch(BaseModel):
    events: list
    # 픽셀로 보낼 event_id 목록 (나머지는 POST /v1/events 배치로 간다)
    pixel_ids: list = []
    # 서명을 일부러 틀리게 할 event_id 목록 (픽셀 경로에서만 의미가 있다)
    forge_ids: list = []


# ==================================================================== API
@app.on_event("startup")
def startup():
    threading.Thread(target=refresh_offsets, daemon=True).start()
    print("[player] ready on :3001", flush=True)


@app.get("/api/health")
def health():
    return {"status": "UP"}


@app.get("/api/bootstrap")
def bootstrap():
    """화면이 뜰 때 한 번. 콘텐츠 목록 + 지금 집행 중인 캠페인."""
    campaigns = []
    try:
        d = _http.get(f"{ADDECISION}/v1/campaigns", timeout=5).json()
        for c in d.get("campaigns", []):
            # 예산 0 인 캠페인은 화면에서 뺀다.
            # 추적기(cmp-tr-*)와 워터마크 밀기(cmp-9999)가 만든 내부용 행들로,
            # 예산 가중 선택에서 뽑히지도 않는다. API 는 있는 그대로 다 내려 주고
            # (거짓말을 하지 않게) 걸러 내는 일은 화면 쪽에서 한다.
            if float(c.get("dailyBudget") or 0) <= 0:
                continue
            campaigns.append({**c, "style": style_for(c.get("vertical"))})
    except Exception as e:
        print(f"[player] 캠페인 조회 실패: {e}", flush=True)
    return {"contents": CONTENTS, "campaigns": campaigns,
            "links": {
                "dashboard": "http://localhost:8088",
                "tracer": "http://localhost:3000",
                "flink": "http://localhost:8181",
                "minio": "http://localhost:9001",
            }}


@app.post("/api/ad-request")
def ad_request(req: AdReq):
    """광고 결정 API 호출.

    여기서 서버가 하는 일은 두 가지다.
      1) ad-decision 의 POST /v1/ad-request 를 그대로 호출한다 (진짜 결정)
      2) 이 광고를 나중에 추적할 수 있게 Kafka 오프셋/분 키를 기억해 둔다
    """
    arid = req.ad_request_id or ("req-play-" + uuid.uuid4().hex[:12])
    t0 = time.time()
    try:
        r = _http.post(f"{ADDECISION}/v1/ad-request", timeout=5, json={
            "ad_request_id": arid,
            "session_id": req.session_id, "user_id": req.user_id,
            "content_id": req.content_id, "device": req.device,
            "ad_pod_id": req.ad_pod_id, "ad_slot": req.ad_slot,
            "playhead_s": req.playhead_s, "max_duration_s": req.max_duration_s,
            "prefer_campaign_id": req.prefer_campaign_id,
            "force_no_fill": req.force_no_fill,
        })
        decision = r.json()
    except Exception as e:
        return JSONResponse({"error": f"ad-decision 호출 실패: {e}"}, 502)

    ts = now_ms()
    now = datetime.fromtimestamp(ts / 1000, timezone.utc)
    with _lock:
        tr = {
            "ad_request_id": arid, "t0": t0,
            "campaign": decision.get("campaign_id"),
            "content_id": req.content_id,
            "minute": minute_key(ts), "dt": now.strftime("%Y-%m-%d"), "hour": now.strftime("%H"),
            "offsets": dict(_offsets_snapshot), "kafka": [], "event_ids": [],
            "redis_base": None,
        }
        tr["redis_base"] = _redis_agg(tr)
        PLAYS[arid] = tr
        for old in list(PLAYS)[:-40]:
            PLAYS.pop(old, None)

    decision["ad_request_id"] = decision.get("ad_request_id") or arid
    decision["style"] = style_for(decision.get("vertical"))
    decision["rtt_ms"] = round((time.time() - t0) * 1000, 1)
    return decision


@app.post("/api/events")
def events(batch: EventBatch):
    """플레이어가 만든 이벤트를 Collector 로 넘긴다.

    반환값에 토픽/전송경로/HTTP 응답을 실어 화면 오른쪽 로그에 그대로 찍는다.
    """
    pixel_ids = set(batch.pixel_ids)
    forge_ids = set(batch.forge_ids)
    results, post_buf = [], []

    for ev in batch.events:
        remember(ev)
        eid = ev.get("event_id")
        et = (ev.get("event_type") or "").lower()
        row = {
            "event_id": eid,
            "event_type": ev.get("event_type"),
            "quartile": ev.get("quartile"),
            "campaign_id": ev.get("campaign_id"),
            "ad_request_id": ev.get("ad_request_id"),
            "source": ev.get("source", "client"),
            "topic": TOPIC_OF.get(et, "dlq.invalid (알 수 없는 타입)"),
            "at": datetime.now().strftime("%H:%M:%S"),
        }
        # 필수 필드가 빠졌으면 Collector 가 검증에서 걸러 dlq.invalid 로 보낸다
        if any(ev.get(f) in (None, "") for f in
               ("event_id", "event_type", "campaign_id", "event_time")):
            row["topic"] = "dlq.invalid (검증 실패)"

        if eid in pixel_ids or eid in forge_ids:
            row["transport"] = "pixel (GET /v1/track)"
            res = send_pixel(ev, forge=eid in forge_ids)
            if eid in forge_ids:
                row["topic"] = "dlq.invalid (서명 위조)"
                row["note"] = "HTTP 는 200 + 1x1 GIF. 집계에는 안 들어간다."
            row.update(res)
            results.append(row)
        else:
            row["transport"] = "batch (POST /v1/events)"
            post_buf.append((ev, row))

    if post_buf:
        res = post_events([e for e, _ in post_buf])
        for _, row in post_buf:
            row.update(res)
            results.append(row)

    return {"results": results}


@app.post("/api/nudge")
def nudge():
    """워터마크 밀기.

    Flink 이벤트타임 윈도우는 워터마크가 윈도우 끝을 지날 때 발화한다.
    사람이 광고 한 편만 보고 가만히 있으면 뒤따르는 이벤트가 없어서
    그 분의 윈도우가 안 닫히고, Redis 단계가 계속 비어 보인다.
    현재 시각 이벤트를 몇 건 흘려 워터마크를 앞으로 민다.
    운영에서는 트래픽이 끊기지 않으므로 이런 장치가 필요 없다.
    """
    ts = now_ms()
    evs = []
    for _ in range(3):
        tag = uuid.uuid4().hex[:8]
        evs.append({"event_id": f"evt-nudge-{tag}", "event_type": "impression",
                    "campaign_id": NUDGE_CAMPAIGN, "event_time": ts,
                    "ad_request_id": f"req-nudge-{tag}", "source": "client"})
    return {"sent": len(evs), **post_events(evs)}


# ============================================================== 단계 확인
def _redis_agg(tr):
    if not tr.get("campaign"):
        return None
    key = f"agg:1m:{tr['campaign']}:{tr['minute']}"
    try:
        h = _rds.hgetall(key)
    except Exception:
        return None
    if not h:
        return None
    return {"key": key,
            "impressions": int(float(h.get("impressions") or 0)),
            "clicks": int(float(h.get("clicks") or 0)),
            "completes": int(float(h.get("completes") or 0)),
            "requests": int(float(h.get("requests") or 0)),
            "ssai_dupes": int(float(h.get("ssai_dupes") or 0))}


def scan_kafka(tr):
    """기억해 둔 오프셋부터 현재 끝까지 훑어 이 광고의 이벤트를 찾는다."""
    targets = {i["event_id"] for i in tr["event_ids"] if i.get("event_id")}
    arid = tr["ad_request_id"]
    try:
        md = _admin.list_topics(timeout=5)
    except Exception:
        return
    with _kafka_lock:
        tps = []
        for key, off in tr["offsets"].items():
            t, p = key.rsplit(":", 1)
            if t in md.topics:
                tps.append(TopicPartition(t, int(p), off))
        if not tps:
            return
        try:
            _consumer.assign(tps)
        except Exception:
            return
        seen = {(m["topic"], m["partition"], m["offset"]) for m in tr["kafka"]}
        deadline = time.time() + 1.5
        while time.time() < deadline:
            msg = _consumer.poll(0.2)
            if msg is None or msg.error():
                continue
            tr["offsets"][f"{msg.topic()}:{msg.partition()}"] = msg.offset() + 1
            try:
                d = json.loads(msg.value())
            except Exception:
                continue
            raw = d.get("raw") if isinstance(d.get("raw"), dict) else {}
            ids = {d.get("event_id"), d.get("ad_request_id"),
                   raw.get("event_id"), raw.get("eid"),
                   raw.get("arid"), raw.get("ad_request_id")}
            if not (ids & targets) and arid not in ids:
                continue
            k = (msg.topic(), msg.partition(), msg.offset())
            if k in seen:
                continue
            seen.add(k)
            tr["kafka"].append({
                "topic": msg.topic(), "partition": msg.partition(), "offset": msg.offset(),
                "at": round(time.time() - tr["t0"], 1),
                "event_id": d.get("event_id") or raw.get("event_id") or raw.get("eid"),
                "event_type": d.get("event_type") or raw.get("event_type") or raw.get("et"),
                "reason": d.get("reason"), "source": d.get("source"),
                "lateness_ms": d.get("lateness_ms"),
            })
        try:
            _consumer.unassign()
        except Exception:
            pass


_PARQUET_CACHE = {}


def check_minio(tr):
    hit = _PARQUET_CACHE.get(tr["ad_request_id"])
    if hit and time.time() - hit[0] < 5:
        return hit[1]
    res = None
    try:
        import pyarrow.dataset as pads
        from pyarrow import fs as pafs
        s3 = pafs.S3FileSystem(endpoint_override=MINIO_HOST, access_key=MINIO_KEY,
                               secret_key=MINIO_SECRET, scheme="http", region="us-east-1")
        path = f"{BUCKET}/dt={tr['dt']}/hour={tr['hour']}"
        ds = pads.dataset(path, filesystem=s3, format="parquet")
        tbl = ds.to_table(columns=["event_id", "ad_request_id", "event_type", "quartile", "source"],
                          filter=pads.field("ad_request_id") == tr["ad_request_id"])
        if tbl.num_rows:
            rows = tbl.to_pylist()
            res = {"rows": tbl.num_rows, "path": f"s3a://{path}/",
                   "types": sorted({(r["event_type"] or "")
                                    + ("/" + r["quartile"] if r.get("quartile") else "")
                                    for r in rows})}
    except Exception as e:
        tr["minio_err"] = str(e)[:160]
    _PARQUET_CACHE[tr["ad_request_id"]] = (time.time(), res)
    return res


def check_pg(tr):
    """Outbox 행(서버가 확정한 ad_response)과 확정 집계(daily_settlement)."""
    out = {"outbox": None, "settlement": None}
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
        with conn, conn.cursor() as cur:
            cur.execute("SELECT id, event_id, published, occurred_at, published_at "
                        "FROM event_outbox WHERE aggregate_id=%s ORDER BY id",
                        (tr["ad_request_id"],))
            rows = cur.fetchall()
            if rows:
                out["outbox"] = [{"id": r[0], "event_id": r[1], "published": r[2],
                                  "occurred_at": str(r[3]),
                                  "published_at": str(r[4]) if r[4] else None}
                                 for r in rows]
            if tr.get("campaign"):
                cur.execute("SELECT impressions, raw_impressions, requests, clicks, amount "
                            "FROM daily_settlement WHERE campaign_id=%s AND dt=%s",
                            (tr["campaign"], tr["dt"]))
                r = cur.fetchone()
                if r:
                    out["settlement"] = {"impressions": r[0], "raw_impressions": r[1],
                                         "requests": r[2], "clicks": r[3],
                                         "amount": float(r[4] or 0)}
        conn.close()
    except Exception as e:
        out["error"] = str(e)[:160]
    return out


@app.get("/api/inspect/{ad_request_id}")
def inspect(ad_request_id: str):
    """이 광고 1편이 지금 어디까지 갔는지 다섯 자리를 한 번에 조회한다."""
    tr = PLAYS.get(ad_request_id)
    if not tr:
        return JSONResponse({"error": "모르는 ad_request_id (서버 재시작 또는 40편 초과)"}, 404)
    scan_kafka(tr)
    agg = _redis_agg(tr)
    base = tr.get("redis_base") or {}
    delta = None
    if agg:
        delta = {"key": agg["key"],
                 "ssai_dupes": agg["ssai_dupes"],
                 **{k: agg[k] - base.get(k, 0) for k in
                    ("impressions", "clicks", "completes", "requests")}}
    pg = check_pg(tr)
    return {
        "ad_request_id": ad_request_id,
        "campaign_id": tr.get("campaign"),
        "elapsed": round(time.time() - tr["t0"], 1),
        "sent": tr["event_ids"],
        "kafka": sorted(tr["kafka"], key=lambda m: m["at"]),
        "redis": delta,
        "redis_hint": None if delta else
            "아직 윈도우가 안 닫혔다. 1분 윈도우 + 워터마크 10초를 기다리거나 [워터마크 밀기] 를 누른다.",
        "minio": check_minio(tr),
        "minio_err": tr.get("minio_err"),
        "postgres": pg,
        "pg_hint": None if pg.get("settlement") else
            "확정 집계는 배치를 돌려야 나온다:  bash scripts/flush-windows.sh && bash scripts/batch.sh",
    }


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "index.html"))
