"""가상 시청자 모델.

실제 시청 행태를 그대로 흉내 내면 사용자 1명이 초당 0.09건 정도밖에 만들지 않는다.
(30초마다 progress ping + 10분마다 광고 브레이크)
로컬에서 200~2000 EPS 를 보려면 사용자를 3만 명 띄우거나 시간을 빨리 감아야 하는데,
후자를 택했다. Clock.speed 가 "콘텐츠 시간 / 실제 시간" 배율이다.

event_time 은 언제나 실제 현재 시각이다. 빨리 감는 것은 시청 위치(playhead)뿐이라
Flink 윈도우/워터마크는 정상 동작한다.
"""
import asyncio
import uuid

from catalog import (AD_LENGTHS, CAMPAIGN_IDS, CAMPAIGN_WEIGHTS, CATALOG,
                     CREATIVES, DEVICE_IDS, DEVICE_WEIGHTS, QUARTILES)
from emitter import now_ms

PROGRESS_INTERVAL_S = 30      # user.behavior progress ping 주기 (콘텐츠 시간)


class Clock:
    """콘텐츠 시간을 실제 시간으로 환산해 재우는 시계."""

    def __init__(self, speed=1.0):
        self.speed = speed

    async def sleep(self, content_seconds):
        await asyncio.sleep(content_seconds / max(self.speed, 0.01))


class VirtualUser:
    def __init__(self, uid, emitter, clock, cfg, rng, ad_decision=None):
        self.uid = uid
        self.em = emitter
        self.clock = clock
        self.cfg = cfg
        self.rng = rng
        self.ad_decision = ad_decision      # None 이면 생성기가 스스로 소재를 정한다
        self.device = rng.choices(DEVICE_IDS, DEVICE_WEIGHTS)[0]
        self.session_id = None
        self.content = None

    # ------------------------------------------------------------ 공통 필드
    def _base(self, event_type, **extra):
        ev = {
            "event_id": "evt-" + uuid.uuid4().hex[:16],
            "event_type": event_type,
            "event_time": now_ms(),
            "user_id": self.uid,
            "session_id": self.session_id,
            "device": self.device,
            "content_id": self.content.content_id if self.content else None,
            "source": "client",
        }
        ev.update(extra)
        return ev

    async def run(self, stop_event):
        # 전원이 같은 순간에 같은 재생위치에서 출발하면 광고 브레이크가 락스텝으로 몰려
        # "광고 이벤트가 몰렸다가 한참 없는" 비현실적인 파형이 나온다. 실제 시간으로도 흩어 준다.
        await asyncio.sleep(self.rng.uniform(0, 3))
        while not stop_event.is_set():
            await self.watch(stop_event)
            # 콘텐츠 사이 쉬는 시간
            await self.clock.sleep(self.rng.uniform(5, 60))

    # -------------------------------------------------------------- 시청
    async def watch(self, stop_event):
        self.content = self.rng.choice(CATALOG)
        self.session_id = "sess-" + uuid.uuid4().hex[:12]
        c = self.content

        # 시작 재생위치. live 는 중간 참여가 기본이고, VOD 도 이어보기가 많다.
        # 이걸 흩뿌려야 광고 브레이크가 사용자마다 다른 시점에 발생한다.
        if c.kind == "live":
            start_pos = self.rng.uniform(0, c.duration_s * 0.9)
        elif self.rng.random() < 0.6:
            start_pos = self.rng.uniform(0, c.duration_s * 0.8)
        else:
            start_pos = 0.0

        # 세션 중도 이탈 지점 (콘텐츠 초). None 이면 끝까지 본다.
        abandon_at = None
        if self.rng.random() < self.cfg.abandon:
            abandon_at = self.rng.uniform(start_pos, c.duration_s)

        await self.em.emit(self._base(
            "session_start",
            campaign_id="none",          # 광고와 무관한 행동 이벤트지만 필수 필드라 채운다
            content_kind=c.kind,
            playhead_s=round(start_pos, 1),
        ), allow_pixel=False)

        # progress ping 지점과 광고 브레이크 지점을 합쳐 하나의 타임라인으로 만든다
        marks = sorted(
            {(p, "ping") for p in range(PROGRESS_INTERVAL_S, c.duration_s, PROGRESS_INTERVAL_S)}
            | {(b, "break") for b in c.ad_breaks}
        )

        pos = start_pos
        for at, kind in marks:
            if at < start_pos:
                continue
            if stop_event.is_set():
                break
            if abandon_at is not None and at > abandon_at:
                await self.em.emit(self._base(
                    "session_end", campaign_id="none",
                    playhead_s=round(abandon_at, 1), reason="abandon",
                ), allow_pixel=False)
                return
            await self.clock.sleep(at - pos)
            pos = at

            if kind == "ping":
                await self.em.emit(self._base(
                    "progress", campaign_id="none", playhead_s=at,
                ), allow_pixel=False)
            else:
                await self.ad_break(at, stop_event)

        if not stop_event.is_set():
            await self.em.emit(self._base(
                "content_end", campaign_id="none", playhead_s=c.duration_s,
            ), allow_pixel=False)
            await self.em.emit(self._base(
                "session_end", campaign_id="none", reason="completed",
            ), allow_pixel=False)

    # ---------------------------------------------------------- 광고 브레이크
    async def ad_break(self, playhead, stop_event):
        """광고 팟 1개. 광고 1~3편이 연속으로 붙는다."""
        pod_size = self.rng.choices([1, 2, 3], [0.45, 0.40, 0.15])[0]
        pod_id = "pod-" + uuid.uuid4().hex[:10]

        for slot in range(pod_size):
            if stop_event.is_set():
                return
            await self.one_ad(pod_id, slot, playhead)

    async def one_ad(self, pod_id, slot, playhead):
        rng = self.rng
        # ad_request_id 가 이 광고 1편의 식별자이자 Kafka 파티션 키다.
        arid = "req-" + uuid.uuid4().hex[:16]

        # 1) 광고 요청 (클라이언트가 본 사실). 항상 Collector 로 보낸다.
        req_ev = self._base(
            "ad_request",
            campaign_id="pending",       # 아직 어떤 캠페인인지 모른다. 결정은 서버가 한다.
            ad_request_id=arid,
            ad_pod_id=pod_id,
            ad_slot=slot,
            playhead_s=round(playhead, 1),
        )
        await self.em.emit(req_ev, allow_pixel=False)

        # 2) 소재 결정.
        #    ad-decision 이 붙어 있으면 서버가 정하고, 그 응답(ad_response)은
        #    Collector 가 아니라 Outbox 를 통해 Kafka 로 들어온다.
        #    없으면 생성기가 자체적으로 정하고 ad_response 도 직접 낸다.
        if self.ad_decision is not None:
            decision = await self.ad_decision.request(
                ad_request_id=arid, session_id=self.session_id, user_id=self.uid,
                content_id=self.content.content_id if self.content else None,
                device=self.device, ad_pod_id=pod_id, ad_slot=slot,
                playhead_s=round(playhead, 1),
            )
            if decision is None:
                return                                    # ad-decision 호출 실패 -> 이 광고는 없던 일
            if not decision.get("fill"):
                return                                    # 노필. impression 이하가 발생하지 않는다.
            campaign = decision["campaign_id"]
            creative = decision.get("creative_id") or rng.choice(CREATIVES.get(campaign, ["crt-unknown"]))
            ad_len = decision.get("ad_duration_s") or rng.choice(AD_LENGTHS)
        else:
            campaign = rng.choices(CAMPAIGN_IDS, CAMPAIGN_WEIGHTS)[0]
            creative = rng.choice(CREATIVES[campaign])
            ad_len = rng.choice(AD_LENGTHS)

        # 서버는 광고를 채웠는데 플레이어에서 실제로 렌더되지 않는 상황.
        # (광고차단, 플레이어 오류, 스티칭 실패) 서버 기준 request 는 남고 impression 만 사라진다.
        # impression/request 비율을 떨어뜨려 단계 4의 이상 감지를 발화시키는 유일한 경로다.
        if rng.random() < self.cfg.drop_impression:
            return

        common = dict(
            campaign_id=campaign,
            ad_request_id=arid,
            creative_id=creative,
            ad_pod_id=pod_id,
            ad_slot=slot,
            ad_duration_s=ad_len,
            playhead_s=round(playhead, 1),
        )

        if self.ad_decision is None:
            await self.em.emit(self._base("ad_response", fill=True, **common), allow_pixel=False)
        await self.clock.sleep(rng.uniform(0.1, 0.4))

        # 3) 임프레션. 여기서 SSAI 이중경로가 갈릴 수 있다.
        await self.em.emit(self._base("impression", **common))

        # 4) quartile. 일부 세션은 도중에 끊긴다.
        drop_from = None
        if rng.random() < self.cfg.ad_abandon:
            drop_from = rng.randint(1, 4)     # start 는 항상 나가고 그 뒤로 끊긴다

        prev_frac = 0.0
        reached = 0
        for i, (name, frac) in enumerate(QUARTILES):
            if drop_from is not None and i >= drop_from:
                break
            await self.clock.sleep((frac - prev_frac) * ad_len)
            prev_frac = frac
            await self.em.emit(self._base("quartile", quartile=name, **common))
            reached = i

        # 5) 클릭. midpoint 이상 본 경우에만.
        if reached >= 2 and rng.random() < self.cfg.click_rate:
            await self.em.emit(self._base("click", **common))
