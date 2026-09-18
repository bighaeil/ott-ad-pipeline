"""콘텐츠/캠페인/디바이스 카탈로그.

campaign_id 는 postgres/init/01-schema.sql 의 시드와 맞춰 둔다.
Flink lookup join(단계 4)과 Spark 정산(단계 5)이 이 값으로 조인한다.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Content:
    content_id: str
    title: str
    kind: str            # vod | live
    duration_s: int      # 콘텐츠 길이(초). live 는 사실상 무한이라 길게 잡는다.
    ad_breaks: tuple     # 광고 브레이크 지점 (콘텐츠 초)


CATALOG = [
    Content("ct-kbo-0912", "KBO 중계 두산:LG", "live", 10800,
            tuple(range(600, 10800, 600))),          # 이닝 사이 10분마다
    Content("ct-drama-201", "드라마 <서울 밤>  12화", "vod", 3600,
            (0, 900, 1800, 2700)),                   # 프리롤 + 미드롤 3회
    Content("ct-movie-77", "영화 <겨울 항구>", "vod", 7200,
            (0, 1800, 3600, 5400)),
    Content("ct-var-33", "예능 <주말의 식탁> 8화", "vod", 4500,
            (0, 1200, 2400, 3600)),
    Content("ct-news-live", "24시 뉴스 라이브", "live", 10800,
            tuple(range(900, 10800, 900))),
]

# 캠페인별 노출 비중. 정산 금액 차이를 만들려고 일부러 기울여 둔다.
CAMPAIGNS = [
    ("cmp-1001", 0.30),
    ("cmp-1002", 0.25),
    ("cmp-1003", 0.20),
    ("cmp-1004", 0.15),
    ("cmp-1005", 0.10),
]
CAMPAIGN_IDS = [c for c, _ in CAMPAIGNS]
CAMPAIGN_WEIGHTS = [w for _, w in CAMPAIGNS]

# 캠페인당 소재 2개씩
CREATIVES = {
    "cmp-1001": ["crt-1001-a", "crt-1001-b"],
    "cmp-1002": ["crt-1002-a", "crt-1002-b"],
    "cmp-1003": ["crt-1003-a", "crt-1003-b"],
    "cmp-1004": ["crt-1004-a", "crt-1004-b"],
    "cmp-1005": ["crt-1005-a", "crt-1005-b"],
}

DEVICES = [
    ("smart_tv", 0.45),
    ("mobile", 0.30),
    ("tablet", 0.10),
    ("pc", 0.10),
    ("stb", 0.05),
]
DEVICE_IDS = [d for d, _ in DEVICES]
DEVICE_WEIGHTS = [w for _, w in DEVICES]

# 광고 길이(초)
AD_LENGTHS = [15, 15, 15, 30]

QUARTILES = [
    ("start", 0.0),
    ("first_quartile", 0.25),
    ("midpoint", 0.50),
    ("third_quartile", 0.75),
    ("complete", 1.0),
]
