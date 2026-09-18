"""1초 단위 콘솔 통계."""
import time
from collections import Counter, deque


class Stats:
    def __init__(self):
        self.total = Counter()        # 누적
        self.window = Counter()       # 최근 1초
        # p95 는 '최근 1초'만 본다. 오래된 표본이 섞이면 값이 얼어붙는다.
        self.latencies = deque(maxlen=5000)
        self.ad_latencies = deque(maxlen=5000)
        self.started = time.monotonic()

    def bump(self, key, n=1):
        self.total[key] += n
        self.window[key] += n

    def latency(self, ms):
        self.latencies.append(ms)

    def latency_ad(self, ms):
        self.ad_latencies.append(ms)

    def _pct(self, p):
        src = getattr(self, "_last_lat", None) or list(self.latencies)
        if not src:
            return 0.0
        s = sorted(src)
        i = min(len(s) - 1, int(len(s) * p))
        return s[i]

    def snapshot(self):
        w = self.window
        self.window = Counter()
        self._last_lat = list(self.latencies)
        self.latencies.clear()
        return w

    def line(self, elapsed, mode, target_eps, speed, active_users, pending, paused):
        w = self.snapshot()
        sent = w["sent"]
        return (
            f"t={elapsed:6.1f}s | {mode:<6} | "
            f"eps {sent:5d}/{target_eps:<5d} | "
            f"tot {self.total['sent']:8d} | "
            f"imp {w['t.impression']:4d} qrt {w['t.quartile']:5d} clk {w['t.click']:3d} "
            f"req {w['t.request']:4d} beh {w['t.behavior']:4d} | "
            f"late {w['a.late']:4d} dup {w['a.dup']:3d} bad {w['a.bad']:3d} "
            f"forge {w['a.forge']:3d} ssai {w['a.ssai']:3d} | "
            f"px {w['sent.pixel']:4d} | "
            f"ad {w['ad.call']:4d}(nf {w['ad.nofill']:3d} er {w['ad.err']:3d}) | "
            f"http2xx {w['http.2xx']:4d} err {w['http.err']:3d} | "
            f"p95 {self._pct(0.95):5.0f}ms | "
            f"x{speed:7.1f} | users {active_users:5d} | buf {pending:6d}"
            + ("  [PAUSED]" if paused else "")
        )

    def summary(self):
        t = self.total
        dur = time.monotonic() - self.started
        lines = [
            "",
            "=" * 78,
            f"  총 {t['sent']} 이벤트 / {dur:.1f}s  (평균 {t['sent'] / max(dur, 1):.0f} eps)",
            f"  경로       : POST {t['sent.post']}  픽셀 {t['sent.pixel']}",
            f"  타입       : impression {t['t.impression']}  quartile {t['t.quartile']}  "
            f"click {t['t.click']}  request/response {t['t.request']}  behavior {t['t.behavior']}",
            f"  이상 주입  : 지연 {t['a.late']}  중복 {t['a.dup']}  스키마위반 {t['a.bad']}  "
            f"서명위조 {t['a.forge']}  SSAI이중 {t['a.ssai']}",
            f"  HTTP       : 2xx {t['http.2xx']}  오류 {t['http.err']}",
            f"  ad-decision: 호출 {t['ad.call']}  fill {t['ad.fill']}  nofill {t['ad.nofill']}  오류 {t['ad.err']}",
            "=" * 78,
        ]
        return "\n".join(lines)
