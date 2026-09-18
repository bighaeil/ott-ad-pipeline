"""agg.minute / late.events / alert.anomaly -> Redis

왜 이 프로세스가 있는가
-----------------------
원래는 Flink SQL 이 집계 결과를 Redis 에 직접 써야 한다.
그런데 Flink 1.20 용 Redis "SQL(Table)" 커넥터가 존재하지 않는다.

  - Apache 공식 Redis 커넥터 없음
  - Apache Bahir flink-connector-redis 1.1.0 : Flink 1.1 시절, 프로젝트 은퇴
  - io.github.jeff-zou:flink-connector-redis 1.4.3 : pom 의 flink.version=1.15.1
  - com.redis:redis-flink-connector 0.0.9 : flink-core 1.19 기반 DataStream 싱크,
                                            Table/SQL 팩토리 없음

임의로 버전을 올려 끼우면 Table API 내부 변경 때문에 런타임에 깨진다.
그래서 Flink 는 Kafka(agg.minute)까지만 내보내고 이 프로세스가 Redis 에 적재한다.
실제 운영에서도 "스트림 → Kafka → 서빙 스토어" 는 흔한 구성이라 크게 어긋나지 않는다.

Redis 키 구조
-------------
  agg:1m:<campaign_id>:<yyyymmddHHMM>   HASH   분단위 집계, TTL 48h
  agg:campaigns                          SET    등장한 campaign_id, TTL 48h
  agg:index                              ZSET   score=window_start epoch, member=위 키
  late:1m:<yyyymmddHHMM>                 STRING 카운터, TTL 48h
  late:total                             STRING 누계
  alerts:recent                          LIST   최근 100건 (JSON), TTL 48h
  alerts:total                           STRING 누계
"""
import json
import os
import signal
import sys
import threading
import time
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

import redis
from confluent_kafka import Consumer, KafkaError

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:29092")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
TTL_SECONDS = int(os.getenv("AGG_TTL_SECONDS", str(48 * 3600)))   # 48시간
GROUP_ID = os.getenv("GROUP_ID", "redis-writer")
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8099"))

TOPICS = ["agg.minute", "late.events", "alert.anomaly"]

STATS = {"agg": 0, "late": 0, "alert": 0, "err": 0, "started": time.time()}
RUNNING = True


def minute_key(iso_ts):
    """'2026-09-12T10:22:00Z' -> '202609121022'"""
    dt = datetime.fromisoformat(iso_ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    return dt.strftime("%Y%m%d%H%M"), int(dt.timestamp())


_REDIS = None


def _r():
    global _REDIS
    if _REDIS is None:
        _REDIS = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                             socket_connect_timeout=5, socket_timeout=5)
    return _REDIS


def _sum_range(start_epoch, end_epoch):
    """agg:index 에서 구간의 키를 뽑아 캠페인별로 합산한다."""
    r = _r()
    keys = r.zrangebyscore("agg:index", start_epoch, end_epoch)
    out = {}
    if not keys:
        return out
    pipe = r.pipeline()
    for k in keys:
        pipe.hgetall(k)
    for h in pipe.execute():
        if not h:
            continue          # TTL 로 사라진 해시 (인덱스만 남은 경우)
        cid = h.get("campaign_id")
        if not cid:
            continue
        acc = out.setdefault(cid, {
            "impressions": 0, "clicks": 0, "completes": 0, "requests": 0,
            "ssai_dupes": 0, "minutes": 0,
            "advertiser": h.get("advertiser", ""), "campaign_name": h.get("campaign_name", ""),
        })
        for f in ("impressions", "clicks", "completes", "requests", "ssai_dupes"):
            acc[f] += int(float(h.get(f) or 0))
        acc["minutes"] += 1
    return out


class Api(BaseHTTPRequestHandler):
    def _json(self, obj, code=200):
        body = json.dumps(obj, ensure_ascii=False).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        from urllib.parse import parse_qs, urlparse
        u = urlparse(self.path)
        q = parse_qs(u.query)
        try:
            if u.path in ("/", "/health"):
                return self._json({
                    "status": "UP",
                    "uptime_s": round(time.time() - STATS["started"], 1),
                    **{k: v for k, v in STATS.items() if k != "started"},
                })

            if u.path == "/agg/summary":
                # 대사(reconciliation) 잡과 대시보드가 읽는다.
                dt = (q.get("dt") or [datetime.now(timezone.utc).strftime("%Y-%m-%d")])[0]
                d0 = datetime.strptime(dt, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                start = int(d0.timestamp())
                end = start + 86400 - 1
                r = _r()
                return self._json({
                    "dt": dt,
                    "campaigns": _sum_range(start, end),
                    "late_total": int(r.get("late:total") or 0),
                    "alerts_total": int(r.get("alerts:total") or 0),
                })

            if u.path == "/agg/series":
                # 대시보드 그래프용. 최근 N분치를 분 단위로.
                mins = int((q.get("minutes") or ["30"])[0])
                now = int(time.time() // 60 * 60)
                start = now - mins * 60
                r = _r()
                keys = r.zrangebyscore("agg:index", start, now + 60, withscores=True)
                pipe = r.pipeline()
                for k, _ in keys:
                    pipe.hgetall(k)
                rows = []
                for (k, score), h in zip(keys, pipe.execute()):
                    if not h:
                        continue
                    rows.append({
                        "minute": int(score),
                        "campaign_id": h.get("campaign_id"),
                        "impressions": int(float(h.get("impressions") or 0)),
                        "clicks": int(float(h.get("clicks") or 0)),
                        "completes": int(float(h.get("completes") or 0)),
                        "requests": int(float(h.get("requests") or 0)),
                        "ssai_dupes": int(float(h.get("ssai_dupes") or 0)),
                    })
                # 지각 이벤트도 분 단위로
                late = {}
                for i in range(mins + 1):
                    m = now - i * 60
                    mk = datetime.fromtimestamp(m, timezone.utc).strftime("%Y%m%d%H%M")
                    v = r.get(f"late:1m:{mk}")
                    if v:
                        late[m] = int(v)
                return self._json({"rows": rows, "late": late})

            if u.path == "/alerts":
                n = int((q.get("limit") or ["20"])[0])
                raw = _r().lrange("alerts:recent", 0, n - 1)
                return self._json({"alerts": [json.loads(x) for x in raw]})

            return self._json({"error": "not found", "path": u.path}, 404)
        except Exception as e:
            return self._json({"error": str(e)}, 500)

    def log_message(self, *args):
        pass


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def serve_health():
    ThreadedHTTPServer(("0.0.0.0", HEALTH_PORT), Api).serve_forever()


def handle_agg(r, d):
    mk, epoch = minute_key(d["window_start"])
    key = f"agg:1m:{d['campaign_id']}:{mk}"
    pipe = r.pipeline()
    pipe.hset(key, mapping={
        "campaign_id": d["campaign_id"],
        "advertiser": d.get("advertiser") or "",
        "campaign_name": d.get("campaign_name") or "",
        "impressions": d.get("impressions") or 0,
        "clicks": d.get("clicks") or 0,
        "completes": d.get("completes") or 0,
        "requests": d.get("requests") or 0,
        "imp_req_ratio": "" if d.get("imp_req_ratio") is None else d["imp_req_ratio"],
        "ssai_dupes": d.get("ssai_dupes") or 0,
        "window_start": d["window_start"],
        "window_end": d["window_end"],
        "updated_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
    })
    pipe.expire(key, TTL_SECONDS)
    pipe.sadd("agg:campaigns", d["campaign_id"])
    pipe.expire("agg:campaigns", TTL_SECONDS)
    pipe.zadd("agg:index", {key: epoch})
    # 48시간보다 오래된 인덱스 항목은 정리한다 (해시는 TTL 로 알아서 사라진다)
    pipe.zremrangebyscore("agg:index", 0, int(time.time()) - TTL_SECONDS)
    pipe.execute()
    STATS["agg"] += 1


def handle_late(r, d):
    ts = d.get("event_time")
    if not ts:
        return
    mk, _ = minute_key(ts)
    key = f"late:1m:{mk}"
    pipe = r.pipeline()
    pipe.incr(key)
    pipe.expire(key, TTL_SECONDS)
    pipe.incr("late:total")
    pipe.execute()
    STATS["late"] += 1


def handle_alert(r, d):
    pipe = r.pipeline()
    pipe.lpush("alerts:recent", json.dumps(d, ensure_ascii=False))
    pipe.ltrim("alerts:recent", 0, 99)
    pipe.expire("alerts:recent", TTL_SECONDS)
    pipe.incr("alerts:total")
    pipe.execute()
    STATS["alert"] += 1


def main():
    global RUNNING
    print(f"[redis-writer] kafka={BOOTSTRAP} redis={REDIS_HOST}:{REDIS_PORT} ttl={TTL_SECONDS}s", flush=True)

    threading.Thread(target=serve_health, daemon=True).start()

    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True,
                    socket_connect_timeout=5, socket_timeout=5)
    for i in range(30):
        try:
            r.ping()
            break
        except Exception as e:
            print(f"[redis-writer] redis 대기 중... {e}", flush=True)
            time.sleep(2)

    consumer = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    })
    consumer.subscribe(TOPICS)

    def stop(*_):
        global RUNNING
        RUNNING = False
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    handlers = {"agg.minute": handle_agg, "late.events": handle_late, "alert.anomaly": handle_alert}
    last_log = time.time()

    while RUNNING:
        msg = consumer.poll(1.0)
        if msg is None:
            pass
        elif msg.error():
            if msg.error().code() != KafkaError._PARTITION_EOF:
                STATS["err"] += 1
                print(f"[redis-writer] kafka 오류: {msg.error()}", flush=True)
        else:
            try:
                d = json.loads(msg.value())
                handlers[msg.topic()](r, d)
            except Exception as e:
                STATS["err"] += 1
                print(f"[redis-writer] 처리 실패 topic={msg.topic()} err={e}", flush=True)

        if time.time() - last_log >= 10:
            last_log = time.time()
            print(f"[redis-writer] agg={STATS['agg']} late={STATS['late']} "
                  f"alert={STATS['alert']} err={STATS['err']}", flush=True)

    consumer.close()
    print("[redis-writer] 종료", flush=True)


if __name__ == "__main__":
    sys.exit(main())
