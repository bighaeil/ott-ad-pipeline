"""OTT 광고 트래픽 시뮬레이터.

  python main.py --mode normal --users 200 --duration 60
  python main.py --mode live   --users 800 --duration 120
  python main.py --mode burst  --users 200 --duration 90

모드별 목표 EPS
  normal  200    평시
  live   2000    KBO 중계 피크
  burst   200    10초간 전송을 멈췄다가 밀린 것을 한꺼번에 보낸다 (지연 폭주)

목표 EPS 는 Clock.speed(콘텐츠 시간 배율)를 1초마다 조정해 맞춘다.
실측 EPS 와 목표가 벌어지면 배율을 곱셈으로 보정하는 단순 제어다.
콘솔에 찍히는 x값이 그 배율이다.
"""
import argparse
import asyncio
import os
import random
import signal
import sys
import time
from dataclasses import dataclass

import aiohttp

from addecision import AdDecisionClient
from emitter import Anomalies, Emitter
from stats import Stats
from users import Clock, VirtualUser

MODE_EPS = {"normal": 200, "live": 2000, "burst": 200}

# 사용자 1명이 콘텐츠 1초당 만드는 이벤트 수의 대략값.
# 초기 배율 추정에만 쓰고, 이후엔 실측으로 보정한다.
EPS_PER_USER_PER_CONTENT_SEC = 0.055


@dataclass
class BehaviorConfig:
    abandon: float = 0.20          # 세션 중도 이탈 비율
    ad_abandon: float = 0.08       # 광고 재생 중 이탈 (quartile 일부만 발생)
    click_rate: float = 0.10       # 광고 1편당 클릭 확률
    drop_impression: float = 0.0   # 서버는 채웠는데 impression 이 안 오는 비율


def parse_args():
    p = argparse.ArgumentParser(description="OTT 광고 이벤트 생성기")
    p.add_argument("--mode", choices=list(MODE_EPS), default="normal")
    p.add_argument("--users", type=int, default=200)
    p.add_argument("--duration", type=int, default=60, help="실행 시간(초). 0 이면 무한.")
    p.add_argument("--eps", type=int, default=0, help="목표 EPS 직접 지정 (모드 기본값 덮어씀)")
    p.add_argument("--collector", default=os.getenv("COLLECTOR_URL", "http://localhost:8080"))
    p.add_argument("--secret", default=os.getenv("COLLECTOR_HMAC_SECRET", "local-dev-secret"))
    p.add_argument("--seed", type=int, default=None)
    p.add_argument("--ad-decision", default=os.getenv("AD_DECISION_URL", ""),
                   help="ad-decision 서비스 URL. 비우면 생성기가 스스로 소재를 정한다.")

    g = p.add_argument_group("이상 케이스 비율")
    g.add_argument("--late", type=float, default=0.10, help="지연 이벤트 (event_time 30~60초 과거)")
    g.add_argument("--dup", type=float, default=0.05, help="같은 event_id 재전송")
    g.add_argument("--bad", type=float, default=0.01, help="필수 필드 누락")
    g.add_argument("--forge", type=float, default=0.01, help="트래킹 픽셀 서명 위조")
    g.add_argument("--ssai", type=float, default=0.03, help="SSAI 클라이언트/서버 이중 경로")

    b = p.add_argument_group("시청 행태")
    b.add_argument("--abandon", type=float, default=0.20)
    b.add_argument("--ad-abandon", type=float, default=0.08)
    b.add_argument("--click-rate", type=float, default=0.10)
    b.add_argument("--drop-impression", type=float, default=0.0,
                   help="서버는 광고를 채웠는데 impression 이 발생하지 않는 비율. "
                        "impression/request 비율을 떨어뜨려 이상 감지를 발화시킨다.")

    t = p.add_argument_group("전송")
    t.add_argument("--pixel-ratio", type=float, default=0.15,
                   help="픽셀(GET /v1/track) 경로로 보낼 비율. 나머지는 배치 POST.")
    t.add_argument("--batch-size", type=int, default=200)
    t.add_argument("--flush-ms", type=int, default=100)

    r = p.add_argument_group("burst 모드")
    r.add_argument("--burst-pause", type=int, default=10, help="전송 정지 시간(초)")
    r.add_argument("--burst-period", type=int, default=30, help="정지 주기(초)")
    return p.parse_args()


async def burst_cycle(em, args, stop):
    """전송만 멈추고 생성은 계속 → 재개 시 밀린 이벤트가 한꺼번에 나간다."""
    run_s = max(args.burst_period - args.burst_pause, 1)
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=run_s)
            return
        except asyncio.TimeoutError:
            pass
        em.pause()
        try:
            await asyncio.wait_for(stop.wait(), timeout=args.burst_pause)
            return
        except asyncio.TimeoutError:
            pass
        em.resume()


async def reporter(stats, clock, em, args, target_eps, stop, n_users):
    """1초마다 통계 출력 + 목표 EPS 추종."""
    t0 = time.monotonic()
    tick = 0
    while not stop.is_set():
        try:
            await asyncio.wait_for(stop.wait(), timeout=1.0)
            return
        except asyncio.TimeoutError:
            pass
        tick += 1
        elapsed = time.monotonic() - t0
        sent_this_sec = stats.window["sent"]

        print(stats.line(elapsed, args.mode, target_eps, clock.speed,
                         n_users, em.pending, em.paused), flush=True)

        # --- 배율 보정 -----------------------------------------------------
        # 이벤트 수는 배율에 거의 선형이라 곱셈 보정이면 충분하다.
        # 처음 2초는 워밍업이라 건너뛴다.
        if tick > 2:
            actual = max(sent_this_sec, 1)
            ratio = target_eps / actual
            ratio = min(max(ratio, 0.5), 2.0)          # 진동 억제
            clock.speed = min(max(clock.speed * (1 + 0.35 * (ratio - 1)), 0.05), 5000.0)


async def main():
    args = parse_args()
    rng = random.Random(args.seed)
    target_eps = args.eps or MODE_EPS[args.mode]

    stats = Stats()
    clock = Clock(speed=max(target_eps / max(args.users * EPS_PER_USER_PER_CONTENT_SEC, 0.01), 0.1))
    cfg = BehaviorConfig(args.abandon, args.ad_abandon, args.click_rate, args.drop_impression)
    anom = Anomalies(args.late, args.dup, args.bad, args.forge, args.ssai)

    print(f"collector    : {args.collector}")
    print(f"mode         : {args.mode}  (목표 {target_eps} eps)")
    print(f"users        : {args.users}   duration: {args.duration or '무한'}s")
    print(f"이상 케이스  : late {args.late:.0%}  dup {args.dup:.0%}  bad {args.bad:.0%}  "
          f"forge {args.forge:.0%}  ssai {args.ssai:.0%}")
    if args.drop_impression:
        print(f"impression 누락: {args.drop_impression:.0%}  (imp/req 비율을 떨어뜨려 alert 발화)")
    print(f"전송         : 픽셀 {args.pixel_ratio:.0%} / 배치 POST {1 - args.pixel_ratio:.0%}  "
          f"(batch {args.batch_size}, flush {args.flush_ms}ms)")
    if args.mode == "burst":
        print(f"burst        : {args.burst_period}초마다 {args.burst_pause}초 전송 정지")
    print(f"ad-decision  : {args.ad_decision or '(미사용 - 생성기가 자체 결정)'}")
    print(f"초기 배율    : x{clock.speed:.1f} (콘텐츠 시간 / 실제 시간)")
    print("-" * 78, flush=True)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:      # Windows
            pass

    connector = aiohttp.TCPConnector(limit=400, ttl_dns_cache=300)
    timeout = aiohttp.ClientTimeout(total=20)
    async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
        em = Emitter(session, args.collector, args.secret, anom, stats, rng,
                     pixel_ratio=args.pixel_ratio, batch_size=args.batch_size,
                     flush_ms=args.flush_ms)
        ad_client = AdDecisionClient(session, args.ad_decision, stats) if args.ad_decision else None

        tasks = [asyncio.create_task(em.flush_loop()),
                 asyncio.create_task(reporter(stats, clock, em, args, target_eps, stop, args.users))]
        if args.mode == "burst":
            tasks.append(asyncio.create_task(burst_cycle(em, args, stop)))

        for i in range(args.users):
            u = VirtualUser(f"user-{i:06d}", em, clock, cfg, random.Random(rng.random()), ad_client)
            tasks.append(asyncio.create_task(u.run(stop)))
            # 전부 동시에 시작하면 첫 초에 몰린다. 살짝 흩어 준다.
            if i % 200 == 199:
                await asyncio.sleep(0.01)

        if args.duration:
            try:
                await asyncio.wait_for(stop.wait(), timeout=args.duration)
            except asyncio.TimeoutError:
                stop.set()
        else:
            await stop.wait()

        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        print("\n남은 버퍼 전송 중...", flush=True)
        await em.close()

    print(stats.summary(), flush=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
