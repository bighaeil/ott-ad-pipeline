"""ad-decision 서비스 클라이언트.

광고 브레이크마다 POST /v1/ad-request 를 호출해 소재를 받아 온다.
서버는 같은 트랜잭션에서 event_outbox 에 ad_response 를 넣고,
그 이벤트는 Collector 를 거치지 않고 Outbox 워커가 직접 Kafka 로 보낸다.

즉 ad.request 토픽에는 두 갈래가 섞여 들어온다.
  ad_request  : 클라이언트가 본 사실 (Collector 경유)
  ad_response : 서버가 확정한 사실 (Outbox 경유)
둘의 개수 차이가 곧 유실/노필/중복이고, 단계 4의 정합성 지표가 이걸 본다.
"""
import time


class AdDecisionClient:
    def __init__(self, session, base_url, stats, timeout_s=3.0):
        self.session = session
        self.url = base_url.rstrip("/") + "/v1/ad-request"
        self.stats = stats
        self.timeout_s = timeout_s

    async def request(self, **payload):
        t0 = time.monotonic()
        try:
            async with self.session.post(self.url, json=payload) as resp:
                if resp.status != 200:
                    self.stats.bump("ad.err")
                    return None
                data = await resp.json()
        except Exception:
            self.stats.bump("ad.err")
            return None

        self.stats.latency_ad((time.monotonic() - t0) * 1000)
        self.stats.bump("ad.call")
        if data.get("fill"):
            self.stats.bump("ad.fill")
        else:
            self.stats.bump("ad.nofill")
        return data
