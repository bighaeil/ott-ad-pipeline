"""관찰 대시보드 API.

한 곳에서 다섯 군데를 긁어 모아 JSON 하나로 준다.

  Collector /actuator/prometheus   수집 카운터
  ad-decision /actuator/prometheus Outbox 상태
  Kafka Admin API                  토픽별 produce 건수 + 컨슈머 랙
  Flink REST /jobs/<id>            연산자별 read/write records
  Redis                            실시간 분단위 집계
  PostgreSQL                       확정 분단위/일단위 집계, 대사 결과

브라우저가 1초마다 /api/overview 를 폴링한다. 매 요청마다 위 여섯 군데를 찌르면
응답이 1초를 넘기므로, 백그라운드 샘플러가 주기적으로 스냅샷을 갱신하고
HTTP 핸들러는 그 스냅샷만 돌려준다.
"""
import json
import os
import re
import socket
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg2
import redis
import requests
from confluent_kafka import (Consumer, ConsumerGroupTopicPartitions,
                             TopicPartition)
from confluent_kafka.admin import AdminClient
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse

COLLECTOR = os.getenv("COLLECTOR_URL", "http://collector:8080")
ADDECISION = os.getenv("AD_DECISION_URL", "http://ad-decision:8090")
FLINK = os.getenv("FLINK_URL", "http://jobmanager:8081")
KAFKA = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
PG = dict(
    host=os.getenv("PG_HOST", "postgres"), port=int(os.getenv("PG_PORT", "5432")),
    dbname=os.getenv("PG_DB", "adplatform"), user=os.getenv("PG_USER", "ads"),
    password=os.getenv("PG_PASSWORD", "ads"),
)
CHART_MINUTES = int(os.getenv("CHART_MINUTES", "30"))
CONSUMER_GROUPS = ["flink-rt", "flink-archive", "redis-writer"]

app = FastAPI(title="ott-ads dashboard")
HERE = os.path.dirname(os.path.abspath(__file__))

SNAP = {"ts": 0, "ready": False}
_prev = {}          # 증가율 계산용 직전 표본
_lock = threading.Lock()

_rds = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True,
                   socket_connect_timeout=3, socket_timeout=3)
_admin = AdminClient({"bootstrap.servers": KAFKA})
_wm = Consumer({"bootstrap.servers": KAFKA, "group.id": "dashboard-wm",
                "enable.auto.commit": False})


# --------------------------------------------------------------------- utils
RATE_WINDOW = 8.0        # 초. Prometheus 계열은 즉시 갱신되므로 짧아도 된다.
RATE_WINDOW_FLINK = 24.0  # Flink REST 는 metrics.fetcher.update-interval(10초)마다만
                          # 갱신되므로 최소 두 번은 담기도록 넉넉히 잡는다.


def rate(key, value, now, window=None):
    """최근 RATE_WINDOW 초 구간의 평균 증가율.

    직전 1초 표본과 비교하면 안 된다. Flink REST 의 read/write-records 는
    metrics.fetcher.update-interval(기본 10초)마다만 갱신되므로 대부분의 틱에서
    값이 그대로고 rate 가 0 으로 찍힌다. 시간창을 두고 평균을 낸다.
    """
    if value is None:
        return 0.0
    w = window or RATE_WINDOW
    buf = _prev.setdefault(key, [])
    buf.append((now, value))
    while len(buf) > 2 and now - buf[0][0] > w:
        buf.pop(0)
    if len(buf) < 2:
        return 0.0
    dt = buf[-1][0] - buf[0][0]
    if dt < 1.0:
        return 0.0
    return max((buf[-1][1] - buf[0][1]) / dt, 0.0)


_METRIC = re.compile(r"^([a-zA-Z_:][\w:]*)(\{[^}]*\})?\s+([-\d.eE+]+)$")


def prom(url):
    """Prometheus 텍스트를 {이름: 라벨무시합계, 이름|라벨키=값: 값} 으로 만든다."""
    out = {}
    try:
        txt = requests.get(url, timeout=2).text
    except Exception:
        return out
    for line in txt.splitlines():
        if not line or line[0] == "#":
            continue
        m = _METRIC.match(line.strip())
        if not m:
            continue
        name, labels, val = m.group(1), m.group(2) or "", m.group(3)
        try:
            v = float(val)
        except ValueError:
            continue
        out[name] = out.get(name, 0.0) + v
        if labels:
            out[name + labels] = v
    return out


def prom_all(base_url):
    """서비스의 모든 복제본을 긁어 합산한다.

    collector 를 --scale 로 늘리면 Docker DNS 가 A 레코드를 여러 개 준다.
    호스트명 하나로 요청하면 라운드로빈이라 매 틱마다 다른 인스턴스의 카운터가
    잡혀 그래프가 널뛴다. A 레코드를 전부 뽑아 각각 긁고 더한다.
    """
    from urllib.parse import urlparse
    u = urlparse(base_url)
    host, port = u.hostname, u.port or 80
    try:
        ips = sorted({ai[4][0] for ai in socket.getaddrinfo(host, port,
                                                            socket.AF_INET, socket.SOCK_STREAM)})
    except Exception:
        ips = [host]
    merged, alive = {}, 0
    for ip in ips:
        m = prom(f"{u.scheme}://{ip}:{port}{u.path}")
        if not m:
            continue
        alive += 1
        for k, v in m.items():
            merged[k] = merged.get(k, 0.0) + v
    return merged, alive, len(ips)


def label_sum(metrics, name, label, value):
    """metric{...label="value"...} 들만 합산."""
    tot = 0.0
    for k, v in metrics.items():
        if k.startswith(name + "{") and f'{label}="{value}"' in k:
            tot += v
    return tot


# ------------------------------------------------------------------ samplers
def sample_ingest(now):
    m, alive, total = prom_all(f"{COLLECTOR}/actuator/prometheus")
    recv = m.get("collector_events_received_total", 0.0)
    inval = m.get("collector_events_invalid_total", 0.0)
    dlq = m.get("collector_dlq_total", 0.0)
    fb = m.get("collector_events_fallback_total", 0.0)
    return {
        "up": bool(m), "replicas": total, "replicas_up": alive,
        "received": int(recv), "received_rate": round(rate("recv", recv, now), 1),
        "invalid": int(inval), "invalid_rate": round(rate("inval", inval, now), 2),
        "dlq": int(dlq), "dlq_rate": round(rate("dlq", dlq, now), 2),
        "fallback": int(fb), "fallback_pending": int(m.get("collector_fallback_pending", 0.0)),
        "kafka_errors": int(m.get("collector_kafka_publish_errors_total", 0.0)),
        "by_reason": {
            k.split('reason="')[1].split('"')[0]: int(v)
            for k, v in m.items() if k.startswith("collector_events_invalid_total{")
        },
    }


def sample_outbox(now):
    m = prom(f"{ADDECISION}/actuator/prometheus")
    pub = m.get("outbox_published_total", 0.0)
    return {
        "up": bool(m),
        "unpublished": int(m.get("outbox_unpublished", 0.0)),
        "lag_seconds": int(m.get("outbox_lag_seconds", 0.0)),
        "published": int(pub), "published_rate": round(rate("outbox_pub", pub, now), 1),
        "republished": int(m.get("outbox_republished_total", 0.0)),
        "update_skipped": int(m.get("outbox_update_skipped_total", 0.0)),
        "fill": int(label_sum(m, "addecision_requests_total", "fill", "true")),
        "nofill": int(label_sum(m, "addecision_requests_total", "fill", "false")),
    }


def sample_kafka(now):
    """토픽별 produce 누계와 컨슈머 그룹 랙."""
    try:
        md = _admin.list_topics(timeout=5)
    except Exception as e:
        return {"error": str(e), "topics": [], "groups": {}}

    ends = {}
    budget = time.time() + 8.0     # 브로커가 죽으면 파티션마다 타임아웃이 쌓인다. 상한을 둔다.
    for t, tm in md.topics.items():
        if t.startswith("__"):
            continue
        total = 0
        for p in tm.partitions:
            if time.time() > budget:
                break
            try:
                _lo, hi = _wm.get_watermark_offsets(TopicPartition(t, p), timeout=1.5, cached=False)
                ends[(t, p)] = hi
                total += hi
            except Exception:
                pass
        ends[t] = total

    topics = []
    for t in sorted(x for x in md.topics if not x.startswith("__")):
        v = ends.get(t, 0)
        topics.append({"topic": t, "produced": v,
                       "rate": round(rate(f"topic:{t}", v, now), 1)})

    # 존재하지 않는 그룹에 list_consumer_group_offsets 를 걸면
    # librdkafka 가 SIGSEGV 로 프로세스를 통째로 죽이는 경우가 있다.
    # (make clean 직후처럼 그룹이 아직 안 생긴 상태) 먼저 존재 여부를 확인한다.
    try:
        # 주의: list_consumer_groups 는 timeout= 이 아니라 request_timeout= 을 받는다.
        # timeout= 을 주면 TypeError 가 나고, 그걸 삼키면 모든 그룹이
        # '없음' 으로 보여 랙이 전부 ? 로 찍힌다.
        listed = _admin.list_consumer_groups(request_timeout=5).result(timeout=6)
        alive_groups = {g.group_id for g in getattr(listed, "valid", [])}
    except Exception:
        alive_groups = set()

    groups = {}
    for g in CONSUMER_GROUPS:
        if g not in alive_groups:
            groups[g] = {"lag": None, "partitions": 0, "note": "그룹 없음"}
            continue
        try:
            fut = _admin.list_consumer_group_offsets(
                [ConsumerGroupTopicPartitions(g, None)])
            res = list(fut.values())[0].result(timeout=5)
            lag = 0
            n = 0
            for tp in (res.topic_partitions or []):
                if tp.offset is None or tp.offset < 0:
                    continue
                hi = ends.get((tp.topic, tp.partition))
                if hi is None:
                    continue
                lag += max(hi - tp.offset, 0)
                n += 1
            groups[g] = {"lag": lag, "partitions": n}
        except Exception:
            groups[g] = {"lag": None, "partitions": 0}
    return {"topics": topics, "groups": groups}


def sample_flink(now):
    """연산자별 read/write records. 체이닝을 꺼 둬야 의미가 있다."""
    out = {"jobs": [], "input": 0, "dedup_in": 0, "dedup_out": 0,
           "dedup_removed": 0, "late_out": 0, "agg_out": 0, "checkpoints": None}
    try:
        ov = requests.get(f"{FLINK}/jobs/overview", timeout=2).json()
    except Exception as e:
        out["error"] = str(e)
        return out

    for j in ov.get("jobs", []):
        out["jobs"].append({"name": j.get("name"), "state": j.get("state"),
                            "running": j.get("tasks", {}).get("running"),
                            "total": j.get("tasks", {}).get("total")})
        if j.get("state") != "RUNNING":
            continue
        try:
            det = requests.get(f"{FLINK}/jobs/{j['jid']}", timeout=3).json()
        except Exception:
            continue
        for v in det.get("vertices", []):
            name = v.get("name", "")
            m = v.get("metrics", {}) or {}
            rin = m.get("read-records") or 0
            rout = m.get("write-records") or 0
            # 입력은 실시간 잡의 소스만 센다. archive 잡은 체이닝으로 Source -> Calc -> Writer 가
            # 한 박스라 write-records 가 이벤트 수가 아니라 체크포인트 커밋 신호 수다
            # (체크포인트 1회당 토픽 5개 = 5건). 그걸 더하면 입력이 부풀려진다.
            if name.startswith("Source:") and j.get("name") == "ott-ads-realtime":
                out["input"] += rout
            if name.startswith("Deduplicate"):
                out["dedup_in"] += rin
                out["dedup_out"] += rout
            if name.startswith("late_events_sink") and "Writer" in name:
                out["late_out"] += rin
            if name.startswith("agg_minute_sink") and "Writer" in name:
                out["agg_out"] += rin
        if j.get("name") == "ott-ads-realtime":
            try:
                cp = requests.get(f"{FLINK}/jobs/{j['jid']}/checkpoints", timeout=3).json()
                c = cp.get("counts", {})
                latest = (cp.get("latest") or {}).get("completed") or {}
                out["checkpoints"] = {
                    "completed": c.get("completed"), "failed": c.get("failed"),
                    "size_kb": round((latest.get("state_size") or 0) / 1024, 1),
                    "duration_ms": latest.get("end_to_end_duration"),
                }
            except Exception:
                pass
    out["dedup_removed"] = max(out["dedup_in"] - out["dedup_out"], 0)
    out["input_rate"] = round(rate("flink_in", out["input"], now, RATE_WINDOW_FLINK), 1)
    out["late_rate"] = round(rate("flink_late", out["late_out"], now, RATE_WINDOW_FLINK), 2)
    return out


def sample_realtime():
    """Redis 분단위 집계 -> 최근 CHART_MINUTES 분."""
    now_m = int(time.time() // 60 * 60)
    start = now_m - CHART_MINUTES * 60
    series = {}
    totals = {}
    try:
        keys = _rds.zrangebyscore("agg:index", start, now_m + 60, withscores=True)
        pipe = _rds.pipeline()
        for k, _ in keys:
            pipe.hgetall(k)
        for (k, score), h in zip(keys, pipe.execute()):
            if not h:
                continue
            cid = h.get("campaign_id")
            if not cid:
                continue
            series.setdefault(cid, {})[int(score)] = int(float(h.get("impressions") or 0))
            t = totals.setdefault(cid, {"impressions": 0, "clicks": 0, "completes": 0,
                                        "requests": 0, "ssai_dupes": 0,
                                        "name": h.get("campaign_name") or cid})
            for f in ("impressions", "clicks", "completes", "requests", "ssai_dupes"):
                t[f] += int(float(h.get(f) or 0))
        late_total = int(_rds.get("late:total") or 0)
        alerts_total = int(_rds.get("alerts:total") or 0)
        alerts = [json.loads(x) for x in _rds.lrange("alerts:recent", 0, 9)]
    except Exception as e:
        return {"error": str(e), "series": {}, "totals": {}, "alerts": [],
                "late_total": 0, "alerts_total": 0}
    return {"series": series, "totals": totals, "alerts": alerts,
            "late_total": late_total, "alerts_total": alerts_total}


def sample_postgres():
    out = {"minute": {}, "daily": [], "recon": [], "outbox_rows": None}
    try:
        conn = psycopg2.connect(connect_timeout=3, **PG)
    except Exception as e:
        out["error"] = str(e)
        return out
    try:
        with conn, conn.cursor() as cur:
            since = datetime.now(timezone.utc) - timedelta(minutes=CHART_MINUTES + 2)
            try:
                cur.execute(
                    "SELECT campaign_id, extract(epoch from minute_ts)::bigint, impressions "
                    "FROM minute_settlement WHERE minute_ts >= %s", (since,))
                for cid, ts, imp in cur.fetchall():
                    out["minute"].setdefault(cid, {})[int(ts)] = int(imp)
            except Exception:
                conn.rollback()      # 아직 배치를 안 돌려 테이블이 없을 수 있다

            cur.execute(
                "SELECT dt::text, campaign_id, requests, impressions, raw_impressions, "
                "       dupes_removed, late_impressions, clicks, completes, amount "
                "FROM daily_settlement ORDER BY dt DESC, campaign_id")
            out["daily"] = [
                {"dt": r[0], "campaign_id": r[1], "requests": r[2], "impressions": r[3],
                 "raw_impressions": r[4], "dupes_removed": r[5], "late_impressions": r[6],
                 "clicks": r[7], "completes": r[8], "amount": float(r[9] or 0)}
                for r in cur.fetchall()]

            cur.execute(
                "SELECT campaign_id, realtime_impressions, batch_impressions, diff, "
                "       diff_rate, likely_cause, run_at "
                "FROM reconciliation WHERE run_at = (SELECT max(run_at) FROM reconciliation) "
                "ORDER BY campaign_id")
            out["recon"] = [
                {"campaign_id": r[0], "realtime": r[1], "batch": r[2], "diff": r[3],
                 "diff_rate": float(r[4] or 0), "cause": r[5],
                 "run_at": r[6].isoformat() if r[6] else None}
                for r in cur.fetchall()]

            cur.execute("SELECT count(*) FILTER (WHERE NOT published), count(*) FROM event_outbox")
            r = cur.fetchone()
            out["outbox_rows"] = {"unpublished": r[0], "total": r[1]}
    except Exception as e:
        out["error"] = str(e)
    finally:
        conn.close()
    return out


def sampler():
    tick = 0
    kafka_snap = {"topics": [], "groups": {}}
    while True:
        t0 = time.time()
        try:
            now = time.time()
            ingest = sample_ingest(now)
            outbox = sample_outbox(now)
            flink = sample_flink(now)
            rt = sample_realtime()
            pg = sample_postgres()
            if tick % 3 == 0:
                kafka_snap = sample_kafka(now)
            snap = {
                "ts": now, "ready": True,
                "ingest": ingest, "outbox": outbox, "flink": flink,
                "kafka": kafka_snap, "realtime": rt, "batch": pg,
                "chart_minutes": CHART_MINUTES,
            }
            with _lock:
                SNAP.clear()
                SNAP.update(snap)
        except Exception as e:
            with _lock:
                SNAP["error"] = str(e)
        tick += 1
        time.sleep(max(1.0 - (time.time() - t0), 0.1))


@app.on_event("startup")
def start():
    threading.Thread(target=sampler, daemon=True).start()


@app.get("/api/overview")
def overview():
    with _lock:
        return JSONResponse(dict(SNAP))


@app.get("/api/health")
def health():
    return {"status": "UP", "ready": SNAP.get("ready", False)}


@app.get("/")
def index():
    return FileResponse(os.path.join(HERE, "index.html"))
