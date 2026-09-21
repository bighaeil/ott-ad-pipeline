"""문서용 화면 캡처 (docs/img/*.png).

시스템에 설치된 Chrome 을 headless 로 쓴다 (Playwright 가 브라우저를 따로 받지 않는다).
사용법과 "어떤 상황을 만들어 두고 찍어야 하는가" 는 같은 폴더의 README.md 참고.

    python scripts/capture/capture.py flink
    python scripts/capture/capture.py minio
    python scripts/capture/capture.py minio-trace
    python scripts/capture/capture.py dash live|settled|alert
    python scripts/capture/capture.py player
    python scripts/capture/capture.py track
    python scripts/capture/capture.py all-live        # flink + minio + dash live + player

    --out DIR 을 주면 docs/img 대신 DIR 에 저장한다 (문서 이미지를 덮어쓰기 전에 확인용).
"""
import argparse
import json
import re
import urllib.request
from pathlib import Path

from playwright.sync_api import sync_playwright

REPO = Path(__file__).resolve().parents[2]
FLINK = "http://localhost:8181"
MINIO = "http://localhost:9001"
DASH = "http://localhost:8088"
PLAYER = "http://localhost:3001"
MINIO_USER = "minioadmin"     # .env 의 MINIO_ROOT_USER / PASSWORD 와 같아야 한다
MINIO_PASS = "minioadmin"

OUT: Path = REPO / "docs" / "img"


def save(target, name, **kw):
    target.screenshot(path=str(OUT / name), **kw)
    print("saved", name)


def rest(path):
    return json.load(urllib.request.urlopen(FLINK + path, timeout=10))


def node_no(name):
    m = re.search(r"\[(\d+)\]", name)
    return int(m.group(1)) if m else 10**9


# --------------------------------------------------------------------------- Flink UI
def flink(page):
    jobs = {j["name"]: j["jid"] for j in rest("/jobs/overview")["jobs"] if j["state"] == "RUNNING"}
    missing = {"ott-ads-realtime", "ott-ads-archive"} - jobs.keys()
    if missing:
        raise SystemExit(f"실행 중이 아닌 잡: {missing} — flink-submit.sh / flink-archive.sh 먼저")
    rt, ar = jobs["ott-ads-realtime"], jobs["ott-ads-archive"]
    verts = {v["name"]: v["id"] for v in rest(f"/jobs/{rt}")["vertices"]}
    dedup = next(i for n, i in verts.items() if n.startswith("Deduplicate"))
    # GlobalWindowAggregate 는 둘이다 (집계 / 경고). 번호가 작은 쪽이 캠페인×분 집계.
    gwin = verts[min((n for n in verts if n.startswith("GlobalWindowAggregate")), key=node_no)]

    def shot(path, name, wait):
        page.goto(FLINK + "/#" + path)
        page.wait_for_timeout(wait)
        save(page, name)

    shot("/overview", "flink-01-overview.png", 4000)
    shot(f"/job/running/{rt}/overview", "flink-02-realtime-graph.png", 6000)
    shot(f"/job/running/{rt}/overview/{dedup}/detail", "flink-03-vertex-detail.png", 5000)
    shot(f"/job/running/{rt}/overview/{gwin}/watermarks", "flink-04-watermarks.png", 5000)
    shot(f"/job/running/{rt}/overview/{dedup}/backpressure", "flink-05-backpressure.png", 6000)
    shot(f"/job/running/{rt}/checkpoints", "flink-06-checkpoints.png", 5000)
    shot(f"/job/running/{rt}/exceptions", "flink-07-exceptions.png", 4000)
    shot(f"/job/running/{ar}/overview", "flink-08-archive-graph.png", 6000)


# --------------------------------------------------------------------------- MinIO 콘솔
def minio_login(page):
    page.goto(MINIO + "/login")
    page.wait_for_timeout(2500)
    page.fill("#accessKey", MINIO_USER)
    page.fill("#secretKey", MINIO_PASS)
    page.keyboard.press("Enter")
    page.wait_for_timeout(4000)


def click_latest(page, prefix):
    """목록에서 prefix 로 시작하는 폴더 중 가장 최근(이름이 가장 큰) 것을 연다."""
    names = [t.strip().rstrip("/") for t in page.get_by_text(re.compile("^" + re.escape(prefix))).all_inner_texts()]
    if not names:
        raise SystemExit(f"MinIO 에 {prefix}* 폴더가 없다 — flink-archive.sh 를 올리고 트래픽을 흘렸는가?")
    latest = max(names)
    page.get_by_text(latest, exact=False).first.click()
    return latest


def minio(page):
    minio_login(page)

    def shot(name):
        page.wait_for_timeout(2500)
        save(page, name)

    page.goto(MINIO + "/browser")
    shot("minio-01-buckets.png")
    page.get_by_text("events", exact=True).first.click()
    page.wait_for_timeout(2500)                      # 버킷 루트 (문서에서 쓰지 않아 저장 안 함)
    print("  dt   =", click_latest(page, "dt="))
    shot("minio-03-dt.png")
    print("  hour =", click_latest(page, "hour="))
    shot("minio-04-hour-files.png")
    page.get_by_text("part-", exact=False).first.click()
    shot("minio-05-object-detail.png")
    page.goto(MINIO + "/buckets/events/admin/summary")
    shot("minio-06-bucket-summary.png")
    page.goto(MINIO + "/tools/metrics")
    shot("minio-07-metrics.png")


def minio_trace(page):
    """Flink 가 체크포인트(10초)마다 파일을 올리는 순간을 잡는다. 22초 = 체크포인트 2번 이상."""
    minio_login(page)
    page.goto(MINIO + "/tools/trace")
    page.wait_for_timeout(2500)
    page.get_by_role("button", name="Start").first.click()
    page.wait_for_timeout(22000)
    save(page, "minio-08-trace.png")


# --------------------------------------------------------------------------- 관찰 대시보드
DASH_SECTIONS = ["ingest", "kafka", "flink", "integrity", "chart", "recon", "alerts"]
# 상황별로 문서에 쓰는 패널 (번호는 1부터)
DASH_SETS = {
    "live": {"full": False, "panels": [1, 2, 3], "wait": 26000},        # 부하 중. Flink 레이트 창(24초)이 차도록
    "settled": {"full": True, "panels": [4, 5, 6], "wait": 9000},       # batch + recon 이후
    "alert": {"full": False, "panels": [4, 7], "wait": 9000},           # impression 유실 부하 중
}


def dash(page, tag):
    cfg = DASH_SETS[tag]
    page.goto(DASH + "/")
    page.wait_for_timeout(cfg["wait"])
    if cfg["full"]:
        save(page, f"dash-{tag}-full.png", full_page=True)
    secs = page.locator("main > section")
    for i in cfg["panels"]:
        save(secs.nth(i - 1), f"dash-{tag}-{i}-{DASH_SECTIONS[i - 1]}.png")


# --------------------------------------------------------------------------- 플레이어
def player(page):
    page.goto(PLAYER + "/#/")
    page.wait_for_timeout(3000)
    save(page, "player-01-home.png", full_page=True)
    page.locator(".tile").first.click()               # 첫 타일 -> 프리롤 광고
    for _ in range(40):
        if page.locator("#adSkip").count():
            break
        page.wait_for_timeout(300)
    page.wait_for_timeout(4000)
    save(page, "player-02-ad.png")
    rail = page.locator("main > div").nth(1)
    save(rail, "player-03-log.png")
    page.get_by_role("button", name="이상 주입").click()
    page.wait_for_timeout(600)
    save(rail, "player-04-inject.png")


def track(page, content_id="ct-drama-201"):
    """광고 1편을 끝까지 보고, 5단계가 모두 채워질 때까지 기다린 뒤 찍는다 (최대 약 2분)."""
    page.goto(f"{PLAYER}/#/watch/{content_id}")
    for _ in range(60):
        if page.locator("#adSkip").count():
            break
        page.wait_for_timeout(500)
    print("  광고 시작")
    for _ in range(80):
        if not page.locator("#adSkip").count():
            break
        page.wait_for_timeout(500)
    arid = page.locator("#adPick").input_value()
    print("  광고 끝, ad_request_id =", arid)
    # 트래픽이 없으면 워터마크가 안 밀려 1분 창이 닫히지 않는다 -> nudge 로 민다 (30초 간격, 최대 3회)
    for i in range(3):
        page.wait_for_timeout(30000)
        urllib.request.urlopen(urllib.request.Request(PLAYER + "/api/nudge", method="POST"), timeout=10).read()
        d = json.load(urllib.request.urlopen(f"{PLAYER}/api/inspect/{arid}", timeout=20))
        print(f"  nudge {i + 1}: redis={'OK' if d.get('redis') else '대기'} minio={'OK' if d.get('minio') else '대기'}")
        if d.get("redis") and d.get("minio"):
            break
    page.wait_for_timeout(4000)
    card = page.locator("#playerView section.card")
    card.scroll_into_view_if_needed()
    page.wait_for_timeout(1500)
    save(card, "player-05-inspect.png")
    page.goto(f"{PLAYER}/#/track/{arid}")
    page.wait_for_timeout(6000)
    save(page, "player-06-track.png", full_page=True)


# ---------------------------------------------------------------------------
def main():
    global OUT
    ap = argparse.ArgumentParser(description="docs/img 캡처")
    ap.add_argument("what", choices=["flink", "minio", "minio-trace", "dash", "player", "track", "all-live"])
    ap.add_argument("tag", nargs="?", choices=list(DASH_SETS), help="dash 일 때 상황: live | settled | alert")
    ap.add_argument("--out", type=Path, help="저장 폴더 (기본: docs/img)")
    a = ap.parse_args()
    if a.what == "dash" and not a.tag:
        ap.error("dash 는 상황을 함께 준다: dash live | settled | alert")
    if a.out:
        OUT = a.out.resolve()
    OUT.mkdir(parents=True, exist_ok=True)
    print("저장 위치:", OUT)

    with sync_playwright() as p:
        b = p.chromium.launch(channel="chrome", headless=True)
        height = 950 if a.what == "track" else 900
        page = b.new_page(viewport={"width": 1440, "height": height}, device_scale_factor=1)
        if a.what in ("flink", "all-live"):
            flink(page)
        if a.what in ("minio", "all-live"):
            minio(page)
        if a.what == "minio-trace":
            minio_trace(page)
        if a.what == "dash":
            dash(page, a.tag)
        if a.what == "all-live":
            dash(page, "live")
        if a.what in ("player", "all-live"):
            player(page)
        if a.what == "track":
            track(page)
        b.close()


if __name__ == "__main__":
    main()
