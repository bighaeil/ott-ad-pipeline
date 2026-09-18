"""플레이어가 보여 주는 콘텐츠 카탈로그.

generator/catalog.py 와 같은 content_id 를 쓴다.
같은 콘텐츠에서 생성기 부하와 사람이 누른 재생이 섞여도
content_id 로 같은 축에서 비교할 수 있게 하기 위해서다.

poster 는 실제 이미지 대신 CSS 그라디언트 두 색이다.
"물리적인 동영상은 없지만 있는 것처럼" 보이게 하는 부분이 전부 여기 들어 있다.
"""

CONTENTS = [
    {
        "content_id": "ct-drama-201",
        "title": "서울 밤",
        "subtitle": "12화 · 끝나지 않은 새벽",
        "kind": "vod",
        "genre": "드라마",
        "rating": "15",
        "year": 2026,
        "duration_s": 3600,
        "ad_breaks": [0, 900, 1800, 2700],
        "poster": ["#1f2a4a", "#6f3d68"],
        "synopsis": "재개발을 앞둔 동네에서 서로를 감시하던 두 사람이 같은 비밀을 나누게 된다.",
    },
    {
        "content_id": "ct-movie-77",
        "title": "겨울 항구",
        "subtitle": "감독판",
        "kind": "vod",
        "genre": "영화",
        "rating": "12",
        "year": 2025,
        "duration_s": 7200,
        "ad_breaks": [0, 1800, 3600, 5400],
        "poster": ["#12303a", "#2e6f7a"],
        "synopsis": "마지막 배가 끊긴 항구에 남은 사람들의 하루.",
    },
    {
        "content_id": "ct-var-33",
        "title": "주말의 식탁",
        "subtitle": "8화 · 가을 제철 밥상",
        "kind": "vod",
        "genre": "예능",
        "rating": "ALL",
        "year": 2026,
        "duration_s": 4500,
        "ad_breaks": [0, 1200, 2400, 3600],
        "poster": ["#4a3a12", "#8a6b1f"],
        "synopsis": "제철 재료 하나로 네 명이 각자의 밥상을 차린다.",
    },
    {
        "content_id": "ct-kbo-0912",
        "title": "KBO 중계 두산:LG",
        "subtitle": "잠실 · 18:30",
        "kind": "live",
        "genre": "스포츠",
        "rating": "ALL",
        "year": 2026,
        "duration_s": 10800,
        "ad_breaks": list(range(600, 10800, 600)),
        "poster": ["#0f3a1f", "#2f7a3f"],
        "synopsis": "이닝 사이마다 광고 브레이크가 들어간다.",
    },
    {
        "content_id": "ct-news-live",
        "title": "24시 뉴스 라이브",
        "subtitle": "생방송",
        "kind": "live",
        "genre": "뉴스",
        "rating": "ALL",
        "year": 2026,
        "duration_s": 10800,
        "ad_breaks": list(range(900, 10800, 900)),
        "poster": ["#2a1f1f", "#6a2f2f"],
        "synopsis": "15분마다 광고가 붙는다.",
    },
]

# 업종(vertical) -> 소재 렌더 색/문구. 광고 결정 API 가 주는 vertical 로 고른다.
VERTICAL_STYLE = {
    "telco":    {"colors": ["#1b3a8f", "#4f7ce8"], "tagline": "지금 가입하면 6개월 반값"},
    "finance":  {"colors": ["#0f3f35", "#2f8f76"], "tagline": "첫 거래 고객 우대금리"},
    "auto":     {"colors": ["#2a2a2a", "#7a5a2a"], "tagline": "사전예약 고객 한정 혜택"},
    "commerce": {"colors": ["#5a1f3a", "#b8467a"], "tagline": "가을 정기세일 최대 70%"},
    "fmcg":     {"colors": ["#3a2a0f", "#a8842f"], "tagline": "3분이면 완성되는 한 끼"},
    "trace":    {"colors": ["#333", "#666"], "tagline": "추적용 캠페인"},
}
DEFAULT_STYLE = {"colors": ["#24303f", "#4a5a70"], "tagline": "광고"}


def style_for(vertical):
    return VERTICAL_STYLE.get(vertical or "", DEFAULT_STYLE)
