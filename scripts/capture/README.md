# 문서 캡처 다시 찍기

`docs/img/*.png` 30장을 만드는 스크립트다. 화면이 바뀌거나 숫자가 오래됐을 때 다시 찍는다.

- 시스템에 설치된 **Chrome** 을 headless 로 쓴다 (`channel="chrome"`). Playwright 가 브라우저를 따로 받지 않는다.
- 스크립트는 **이미 만들어진 상황을 찍기만** 한다. 부하·배치·사고 같은 상황은 아래 순서대로 먼저 만들어 둔다.
  빈 화면은 설명이 되지 않는다.

## 준비 (한 번)

```bash
python -m venv .venv-capture
.venv-capture/Scripts/python -m pip install -r scripts/capture/requirements.txt   # Windows
# macOS/Linux: .venv-capture/bin/python -m pip install -r scripts/capture/requirements.txt
```

`.venv-capture/` 는 `.gitignore` 대상이다. 아래 예시의 `py` 는 이 venv 의 python 을 뜻한다.

## 상황별로 찍기

먼저 확인용 폴더에 찍어서 본 다음, 괜찮으면 `--out` 없이 다시 찍어 `docs/img` 를 덮어쓴다.

```bash
py scripts/capture/capture.py flink --out /tmp/shots
```

| 순서 | 만들어 둘 상황 | 명령 | 결과 |
|---|---|---|---|
| 0 | `make up` → `flink-submit.sh` → `flink-archive.sh` | — | — |
| 1 | **부하 중** — `USERS=80 DURATION=420 bash scripts/load.sh` 를 다른 창에서 돌리고 1분 이상 지난 뒤 | `capture.py all-live` | flink-01~08, minio-01·03~07, dash-live-1~3, player-01~04 |
| 2 | 부하 중 (1과 같은 상황) | `capture.py minio-trace` | minio-08 (22초 걸림) |
| 3 | 부하 **없음** (다른 트래픽이 섞이면 추적 숫자가 흐려짐) | `capture.py track` | player-05·06 (최대 약 2분) |
| 4 | 부하 끝난 뒤 `flush-windows.sh` → `batch.sh` → `recon.sh` | `capture.py dash settled` | dash-settled-full, -4·5·6 |
| 5 | **사고 중** — `USERS=200 DURATION=170 bash scripts/load.sh --drop-impression 0.7` 시작 후 1~2분 | `capture.py dash alert` | dash-alert-4·7 |

## 찍은 뒤 확인할 것

- **한 장씩 열어 본다.** 값이 0인 패널, 로딩 중 화면, 잘린 표가 찍혔으면 상황을 다시 만들고 찍는다.
- 문서 본문이 캡처 속 숫자를 인용하는 곳이 있다 (예: `docs/07-flink-guide.md` 의 `117 → 44`,
  `docs/09-dashboard-guide.md` 의 `0.71~0.82 / 0.18~0.35`). 새로 찍으면 그 숫자도 같이 고친다.
- `minio-02` 는 본문에서 쓰지 않아 만들지 않는다 (번호가 01 → 03 으로 건너뛴다).

## 알아 둘 것

- MinIO 는 `dt=` / `hour=` 폴더 중 **가장 최근 것**을 자동으로 연다.
- Flink 워터마크 캡처(flink-04)는 `GlobalWindowAggregate` 두 개 중 번호가 작은 쪽(캠페인×분 집계)을 연다.
  SQL 을 고쳐 노드 번호가 바뀌어도 동작하지만, 집계 순서가 바뀌면 확인이 필요하다.
- MinIO 계정은 스크립트 상단 `MINIO_USER` / `MINIO_PASS` (기본 `minioadmin`). `.env` 를 바꿨으면 같이 바꾼다.
