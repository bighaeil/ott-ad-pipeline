"""이벤트 전송 + 이상 케이스 주입.

두 개의 전송 경로를 흉내 낸다.
  POST /v1/events : 플레이어 SDK 가 이벤트를 모아 보내는 경로
  GET  /v1/track  : VAST 트래킹 픽셀 경로 (HMAC 서명 필요)

주입하는 이상 케이스 (비율은 전부 CLI 인자)
  late   지연     event_time 이 30~60초 과거. 네트워크 복구 후 밀린 전송.
  dup    중복     같은 event_id 를 잠시 뒤 한 번 더 보낸다.
  bad    스키마   필수 필드 하나를 뺀다.
  forge  서명위조 픽셀에 엉뚱한 sig 를 붙인다.
  ssai   이중경로 같은 ad_request_id 의 impression 이 client/server 두 경로로 도착.
                  event_id 는 서로 다르다 -> event_id 중복제거로는 안 잡힌다.
                  이게 dup 과 ssai 를 나눠 둔 이유다.
"""
import asyncio
import hashlib
import hmac
import random
import time
import uuid
from urllib.parse import urlencode

import aiohttp

REQUIRED_FIELDS = ["event_id", "event_type", "campaign_id", "event_time"]

# 이벤트 타입 -> 통계 버킷
_TYPE_BUCKET = {
    "impression": "t.impression",
    "quartile": "t.quartile",
    "click": "t.click",
    "ad_request": "t.request",
    "ad_response": "t.request",
}

# 픽셀 파라미터 <- 이벤트 필드
_PIXEL_MAP = {
    "eid": "event_id",
    "et": "event_type",
    "cid": "campaign_id",
    "ts": "event_time",
    "arid": "ad_request_id",
    "crid": "creative_id",
    "q": "quartile",
    "sid": "session_id",
    "src": "source",
}


def now_ms():
    return int(time.time() * 1000)


class Anomalies:
    def __init__(self, late=0.10, dup=0.05, bad=0.01, forge=0.01, ssai=0.03):
        self.late = late
        self.dup = dup
        self.bad = bad
        self.forge = forge
        self.ssai = ssai


class Emitter:
    def __init__(self, session: aiohttp.ClientSession, collector_url: str, secret: str,
                 anomalies: Anomalies, stats, rng: random.Random,
                 pixel_ratio=0.15, batch_size=200, flush_ms=100):
        self.session = session
        self.url_events = collector_url.rstrip("/") + "/v1/events"
        self.url_track = collector_url.rstrip("/") + "/v1/track"
        self.secret = secret.encode()
        self.anom = anomalies
        self.stats = stats
        self.rng = rng
        self.pixel_ratio = pixel_ratio
        self.batch_size = batch_size
        self.flush_ms = flush_ms

        self._buf = []
        self._pixel_buf = []
        self._paused = False          # burst 모드에서 전송만 멈춘다 (생성은 계속)
        self._closing = False
        self._inflight = set()

    # ------------------------------------------------------------- 전송 제어
    def pause(self):
        self._paused = True

    def resume(self):
        self._paused = False

    @property
    def pending(self):
        return len(self._buf) + len(self._pixel_buf)

    @property
    def paused(self):
        return self._paused

    # --------------------------------------------------------------- emit
    async def emit(self, ev: dict, allow_pixel=True):
        """이벤트 1건. 여기서 이상 케이스를 주입한다."""
        r = self.rng

        # (1) SSAI 이중경로 - impression 한정.
        #     클라이언트 경로로 이미 온 impression 을 SSAI 스티처가 서버 경로로 한 번 더 보낸다.
        #     event_id 는 새로 발급되므로 event_id 기준 중복제거로는 걸러지지 않는다.
        if ev.get("event_type") == "impression" and r.random() < self.anom.ssai:
            twin = dict(ev)
            twin["event_id"] = "evt-" + uuid.uuid4().hex[:16]
            twin["source"] = "server"
            twin["ssai_twin_of"] = ev["event_id"]
            self.stats.bump("a.ssai")
            await self._route(twin, allow_pixel=False)   # 서버 경로는 배치 POST

        # (2) 지연 - 30~60초 과거 타임스탬프
        if r.random() < self.anom.late:
            ev["event_time"] = ev["event_time"] - int(r.uniform(30, 60) * 1000)
            ev["_late"] = True
            self.stats.bump("a.late")

        # (3) 서명 위조 - 픽셀 경로에서만 성립
        forge = r.random() < self.anom.forge

        # (4) 스키마 위반 - 필수 필드 하나 제거.
        #     픽셀 경로는 이 뒤에 서명하므로 "서명은 맞는데 필드가 빈" 케이스가 된다.
        if r.random() < self.anom.bad:
            victim = r.choice(REQUIRED_FIELDS)
            ev.pop(victim, None)
            ev["_bad"] = victim
            self.stats.bump("a.bad")

        await self._route(ev, allow_pixel=allow_pixel, forge=forge)

        # (5) 중복 - 같은 event_id 를 0.5~3초 뒤에 한 번 더.
        #     같은 배치가 아니라 다른 배치로 가야 실제 재전송과 비슷하다.
        if r.random() < self.anom.dup:
            self.stats.bump("a.dup")
            copy = dict(ev)
            copy["_dup"] = True
            delay = r.uniform(0.5, 3.0)
            task = asyncio.create_task(self._delayed(copy, delay, allow_pixel))
            self._inflight.add(task)
            task.add_done_callback(self._inflight.discard)

    async def _delayed(self, ev, delay, allow_pixel):
        try:
            await asyncio.sleep(delay)
            if not self._closing:
                await self._route(ev, allow_pixel=allow_pixel)
        except asyncio.CancelledError:
            pass

    async def _route(self, ev, allow_pixel=True, forge=False):
        bucket = _TYPE_BUCKET.get(ev.get("event_type"), "t.behavior")
        self.stats.bump(bucket)
        self.stats.bump("sent")

        use_pixel = forge or (allow_pixel and self.rng.random() < self.pixel_ratio)
        if use_pixel:
            if forge:
                self.stats.bump("a.forge")
            self._pixel_buf.append((ev, forge))
        else:
            self._buf.append(_strip(ev))

    # -------------------------------------------------------------- 플러시
    async def flush_loop(self):
        while not self._closing:
            await asyncio.sleep(self.flush_ms / 1000)
            await self._flush(force=False)

    async def _flush(self, force):
        if self._paused and not force:
            return
        if self._buf:
            batch, self._buf = self._buf, []
            for i in range(0, len(batch), self.batch_size):
                self._spawn(self._post(batch[i:i + self.batch_size]))
        if self._pixel_buf:
            pixels, self._pixel_buf = self._pixel_buf, []
            for ev, forge in pixels:
                self._spawn(self._pixel(ev, forge))

    def _spawn(self, coro):
        task = asyncio.create_task(coro)
        self._inflight.add(task)
        task.add_done_callback(self._inflight.discard)

    async def _post(self, batch):
        t0 = time.monotonic()
        try:
            async with self.session.post(self.url_events, json=batch) as resp:
                await resp.read()
                self.stats.latency((time.monotonic() - t0) * 1000)
                self.stats.bump("sent.post", len(batch))
                if 200 <= resp.status < 300:
                    self.stats.bump("http.2xx", len(batch))
                else:
                    self.stats.bump("http.err", len(batch))
        except Exception:
            self.stats.bump("http.err", len(batch))

    async def _pixel(self, ev, forge):
        params = {}
        for pk, ek in _PIXEL_MAP.items():
            v = ev.get(ek)
            if v is not None:
                params[pk] = str(v)
        params["exp"] = str(int(time.time()) + 300)

        if forge:
            params["sig"] = uuid.uuid4().hex[:32]
        else:
            params["sig"] = self.sign(params)

        t0 = time.monotonic()
        try:
            async with self.session.get(self.url_track + "?" + urlencode(params)) as resp:
                await resp.read()
                self.stats.latency((time.monotonic() - t0) * 1000)
                self.stats.bump("sent.pixel")
                if 200 <= resp.status < 300:
                    self.stats.bump("http.2xx")
                else:
                    self.stats.bump("http.err")
        except Exception:
            self.stats.bump("http.err")

    def sign(self, params):
        """Collector 의 SignatureVerifier 와 동일 규칙.
        canonical = sig 제외 파라미터를 key 오름차순으로 k=v 연결, & 구분 (값은 디코딩 상태).
        """
        canonical = "&".join(f"{k}={params[k]}" for k in sorted(params) if k != "sig")
        return hmac.new(self.secret, canonical.encode(), hashlib.sha256).hexdigest()

    async def close(self):
        self._closing = True
        self.resume()
        await self._flush(force=True)
        if self._inflight:
            await asyncio.gather(*list(self._inflight), return_exceptions=True)


def _strip(ev):
    """관찰용 내부 표식(_late/_bad/_dup)은 붙여서 보낸다.
    Collector 는 모르는 필드를 그대로 통과시키므로, 나중에 Kafka 원문에서
    '이건 일부러 넣은 이상 케이스였다'를 되짚을 수 있다.
    """
    return ev
