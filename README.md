# OTT 광고 이벤트 수집·집계 파이프라인 (로컬)

수집부터 정산 집계까지 전 구간을 로컬 Docker 에 올려, 아키텍처가 실제로 어떻게
움직이는지 관찰하기 위한 환경이다. 학습 예제가 아니라 **관찰 장치**가 목적이므로
지연·중복·유실·정합성 어긋남이 일부러 발생하도록 만들어져 있다.

```
  [플레이어 시뮬레이터] ─┐
  [SSAI 스티처 모사]   ─┤
  [Outbox 워커]        ─┘
            │
            ▼
     Collector API (Kotlin + Spring Boot WebFlux)
            │  검증 → Kafka. 실패 시 로컬파일 fallback
            ▼
          Kafka
       ┌────┴────┐
       ▼         ▼
    Flink     Kafka Connect → MinIO(S3) Parquet
       │                    │
       ▼                    ▼
     Redis              Spark 배치 → PostgreSQL
       │                                │
       └────────┬───────────────────────┘
                ▼
          모니터링 대시보드 (웹)
```

**진행 상태**: 단계 1~7 전부 완료 + 플레이어 화면(단계 8)

### 문서

이 README 는 **단계별 구현 기록**이다. 주제별 정리는 [`docs/`](docs/) 에 있다.

| 문서 | 무엇에 답하나 |
|---|---|
| [docs/01-event-flow.md](docs/01-event-flow.md) | 이벤트가 태어나서 정산될 때까지의 모든 홉 (다이어그램) |
| [docs/02-data-stores.md](docs/02-data-stores.md) | 원본이 어디에 어떤 스키마로 쌓이고 누가 읽나 |
| [docs/03-ad-decision-api.md](docs/03-ad-decision-api.md) | 광고 결정 API 규격 + Outbox + 운영 확장 |
| [docs/04-scale-100m.md](docs/04-scale-100m.md) | 1억 건 규모에서 무엇이 먼저 깨지고 무엇을 바꾸나 |
| [docs/05-kafka-guide.md](docs/05-kafka-guide.md) | Kafka 구조·개념·설계 판단 + 학습 자료 |
| [docs/06-learning-path.md](docs/06-learning-path.md) | 4주 학습 로드맵·실습 과제·면접 질문 |
| [docs/07-flink-guide.md](docs/07-flink-guide.md) | Flink 역할 + Flink UI(:8181) 읽는 법 (캡처) |
| [docs/08-minio-guide.md](docs/08-minio-guide.md) | MinIO 역할 + 콘솔(:9001)·`mc` 로 원본 보는 법 (캡처) |
| [docs/09-dashboard-guide.md](docs/09-dashboard-guide.md) | 관찰 대시보드(:8088) 패널별 읽는 법 (캡처) |

---

## 0. 준비

필요한 것: Docker Desktop (Compose v2 이상), bash(Git Bash 또는 WSL), curl, openssl.

### `make` 가 없어도 된다 — 명령 대응표

**Windows 에는 `make` 가 기본으로 없다.** 이 문서는 짧아서 `make xxx` 로 적었지만,
실제로는 전부 `bash scripts/xxx.sh` 를 부르는 단축키일 뿐이다.
`make: command not found` 가 나오면 오른쪽 열을 그대로 쓰면 된다.

| 하고 싶은 것 | make 있을 때 | **make 없을 때 (Git Bash)** |
|---|---|---|
| 전체 기동 | `make up` | `docker compose up -d && bash scripts/health.sh` |
| 동작 확인 | `make smoke` | `bash scripts/smoke.sh` |
| 실시간 잡 제출 | `make flink` | `bash scripts/flink-submit.sh` |
| 원본 적재 잡 제출 | `make archive` | `bash scripts/flink-archive.sh` |
| 트래픽 생성 | `make load` | `bash scripts/load.sh` |
| 윈도우 닫기 | `make flush` | `bash scripts/flush-windows.sh` |
| 정산 배치 | `make batch` | `bash scripts/batch.sh` |
| 대사 | `make recon` | `bash scripts/recon.sh` |
| 헬스 확인 | `make health` | `bash scripts/health.sh` |
| CLI 관찰 | `make observe` | `bash scripts/observe.sh` |
| 잡 취소 | `make flink-cancel` | `bash scripts/flink-cancel.sh` |
| 시나리오 5종 | `make scenario-*` | `bash scripts/scenario_*.sh` |
| 정지 | `make down` | `docker compose --profile batch --profile load down` |
| 초기화 | `make clean` | `docker compose --profile batch --profile load down -v` |
| psql | `make psql` | `docker compose exec postgres psql -U ads -d adplatform` |
| redis-cli | `make redis-cli` | `docker compose exec redis redis-cli` |

PowerShell 을 쓴다면 `make.ps1` 이 같은 타깃을 제공한다.

```powershell
powershell -ExecutionPolicy Bypass -File .\make.ps1 up
powershell -ExecutionPolicy Bypass -File .\make.ps1 smoke
```

> **Git Bash 주의**: MSYS 가 `/opt/kafka/...` 같은 인자를 Windows 경로로 자동 변환해
> `docker compose exec` 가 깨진다. 스크립트와 Makefile 에 `MSYS_NO_PATHCONV=1` 을
> 넣어 두었으니, 직접 `docker compose exec` 를 칠 때는 같은 변수를 앞에 붙일 것.

### Windows 에서 일부 서비스가 안 뜰 때 — 포트 예약 문제

**증상**: `docker compose up -d` 가 일부 서비스에서 이렇게 실패한다. 그 포트를 쓰는 프로세스는 없다.

```
Error response from daemon: ports are not available: exposing port TCP 0.0.0.0:8181 …
bind: An attempt was made to access a socket in a way forbidden by its access permissions.
```

**원인**: Windows(Hyper-V / WinNAT)가 TCP 포트 구간을 통째로 예약해 둔 것이다.
예약 구간은 재부팅할 때마다 바뀌어서, 어제 되던 것이 오늘 안 될 수 있다.

```powershell
netsh interface ipv4 show excludedportrange protocol=tcp
```

실제로 겪은 예약 구간은 `8068–8167`, `8168–8267` 이었고, 이 프로젝트의 호스트 포트 중
**8080(collector) · 8088(dashboard) · 8090(ad-decision) · 8099(redis-writer) · 8181(Flink UI)**
다섯 개가 전부 여기에 들어갔다. 컨테이너끼리의 통신(`collector:8080` 등)은 영향이 없고,
**호스트로 포트를 게시할 때만** 막힌다.

**해결** (관리자 PowerShell):

```powershell
# ① 당장 풀기 — 재부팅 후 다시 걸릴 수 있다
net stop winnat
net start winnat

# ② 영구히 막기 — 필요한 포트를 먼저 예약해 두면 Windows 가 가져가지 않는다
net stop winnat
foreach ($p in 8080,8088,8090,8099,8181) {
  netsh int ipv4 add excludedportrange protocol=tcp startport=$p numberofports=1 store=persistent
}
net start winnat
```

②를 한 번 해 두는 것을 권장한다. 포트를 구간 밖으로 옮기는 방법도 있지만,
`localhost:8080` 같은 주소가 스크립트와 문서 여러 곳에 박혀 있어 고칠 곳이 많다.

> **오버라이드 파일로 포트만 바꿔 띄우고 싶다면** `ports:` 앞에 `!override` 를 붙인다.
> Compose 는 파일을 겹칠 때 목록을 **덧붙이므로** 태그 없이 쓰면 원래 포트도 남아 똑같이 실패하고,
> `!reset` 은 속성을 **비우기만** 해서 새 값이 무시된다(게시 자체가 사라진다).
> `docker-compose.scale.yml` 이 `!override` 를 쓰는 예다.

---

## 1. 실행 순서

`make` 가 없으면 오른쪽 주석의 명령을 쓴다 (§0 대응표).

```bash
make up        # bash: docker compose up -d && bash scripts/health.sh
make smoke     # bash: bash scripts/smoke.sh
make flink     # bash: bash scripts/flink-submit.sh
make archive   # bash: bash scripts/flink-archive.sh
make load      # bash: bash scripts/load.sh
```

브라우저에서 **http://localhost:8088** — 여기서 숫자가 움직인다.

```bash
make flush     # bash: bash scripts/flush-windows.sh
make batch     # bash: bash scripts/batch.sh
make recon     # bash: bash scripts/recon.sh
```

기동 후 접속 지점

| 대상 | 주소 |
|---|---|
| **관찰 대시보드** | **http://localhost:8088** |
| **이벤트 추적기** | **http://localhost:3000** |
| Flink JobManager UI | http://localhost:8181 |
| MinIO 콘솔 | http://localhost:9001 (minioadmin / minioadmin) |
| Collector | http://localhost:8080/actuator/health |
| Collector 메트릭 | http://localhost:8080/actuator/prometheus |
| ad-decision | http://localhost:8090/actuator/health |
| Outbox 상태 | http://localhost:8090/v1/outbox/stats |
| redis-writer 상태 | http://localhost:8099/ |
| Kafka (호스트에서) | localhost:29092 |
| PostgreSQL | localhost:5432 (ads / ads / adplatform) |
| Redis | localhost:6379 |

---

## 1-2. 관찰 가이드 — 무엇을 어디서 보는가

### A. 웹 화면 (브라우저)

| 화면 | 주소 | 무엇을 보나 |
|---|---|---|
| **플레이어 (OTT 화면)** | **http://localhost:3001** | **콘텐츠를 재생하고 광고를 본다. 어떤 광고인지(광고주/캠페인/소재/CPM)가 화면에 뜨고, 그 광고가 쌓은 데이터를 바로 조회한다** |
| **관찰 대시보드** | **http://localhost:8088** | **전 구간을 한 페이지에서. 1초 갱신 — 전체가 얼마나 흐르나** |
| **이벤트 추적기** | **http://localhost:3000** | **버튼 클릭 → 이벤트 1건이 어디까지 갔는지 단계별 추적** |
| Flink JobManager UI | http://localhost:8181 | 잡 상태, 연산자 그래프, 체크포인트 이력, 백프레셔 |
| MinIO 콘솔 | http://localhost:9001 | Parquet 원본 파일. 로그인 `minioadmin` / `minioadmin` |

대시보드 헤더에 나머지 화면으로 가는 링크가 붙어 있다.

### B. JSON / 메트릭 (브라우저에서 그냥 열린다)

| 주소 | 내용 |
|---|---|
| http://localhost:8088/api/overview | 대시보드가 그리는 원본 데이터 전부 |
| http://localhost:8080/actuator/prometheus | Collector 카운터 (수신/검증실패/fallback/DLQ) |
| http://localhost:8080/v1/admin/fallback | fallback 파일 상태 |
| http://localhost:8090/v1/outbox/stats | Outbox 미발행 건수 / 최고 지연 |
| http://localhost:8090/v1/campaigns | 지금 집행 중인 캠페인 (광고 결정 API) |
| http://localhost:8090/actuator/prometheus | ad-decision 메트릭 |
| http://localhost:8099/agg/summary | Redis 실시간 집계 요약 (캠페인별) |
| http://localhost:8099/alerts?limit=10 | 최근 이상 감지 경고 |

### C. 관리자 콘솔 (CLI)

```bash
make psql        # PostgreSQL. 확정 집계 / 대사 / Outbox 원장
make redis-cli   # Redis. 실시간 분단위 집계
make topics      # Kafka 토픽 describe
make observe     # 대시보드의 CLI 버전 (2초 간격 텍스트)
make health      # 전 컴포넌트 헬스 한 번에
```

```bash
make consume T=ad.impression N=5   # 토픽 실물 꺼내 보기
```

자주 쓰는 SQL:

```sql
-- 확정 집계 (정산)
select dt, campaign_id, requests, impressions, raw_impressions,
       dupes_removed, late_impressions, amount
from daily_settlement order by campaign_id;

-- 대사 결과 (실행 이력이 쌓인다)
select campaign_id, realtime_impressions, batch_impressions, diff, diff_rate, likely_cause
from reconciliation where run_at = (select max(run_at) from reconciliation);

-- Outbox 원장 (published=false 가 아직 Kafka 로 안 간 것)
select published, count(*) from event_outbox group by published;
```

---

### 처음 보는 사람을 위한 20분 코스

브라우저에 **http://localhost:8088** 을 띄워 놓고 터미널에서 아래를 순서대로 친다.
각 단계마다 화면의 어느 칸이 움직이는지 적어 두었다.

**0) 올린다** — 첫 실행은 이미지 빌드 때문에 10분쯤 걸린다.

```bash
make up
```

`make health` 가 9줄 전부 초록 `OK` 면 준비 끝.

**1) 이벤트 1건이 끝까지 가는지 본다**

```bash
make smoke
```

3건을 넣고 Kafka 에서 다시 꺼내 보여 준다. 필수 필드가 빠진 1건과 서명이 틀린 픽셀 1건이
`dlq.invalid` 로 가는 것까지 한 화면에 나온다.
→ 대시보드 `[수집]` 의 **누적 수신 +5, DLQ +2**.

**2) 실시간 처리를 켠다**

```bash
make flink
```

```bash
make archive
```

→ 대시보드 `[처리]` 에 `ott-ads-realtime 34/34 tasks`, `ott-ads-archive 10/10 tasks` 가 뜬다.
→ **여기까지 안 하면 `late.events` / `agg.minute` / Redis 가 계속 0 이다.** 고장이 아니다.

**3) 트래픽을 흘린다 — 여기서부터 숫자가 움직인다**

```bash
make load
```

브라우저를 보면서 1분쯤 기다린다.

| 대시보드 칸 | 무엇이 보이나 |
|---|---|
| `[수집]` 초당 수신 | 0 → 200 대로 |
| `[버퍼]` 토픽 막대 | `user.behavior` 가 가장 굵고 `ad.quartile` 이 그다음 |
| `[버퍼]` consumer lag | `flink-rt` / `flink-archive` 가 벌어졌다 좁혀진다 |
| `[처리]` 입력 건/초 | 수집보다 **몇 초 늦게** 따라 올라온다 |
| `[처리]` 중복제거 | `14,202→13,576` 처럼 in/out 이 벌어진다 = 걷어낸 중복 |
| `[처리]` late.events | 생성기가 심은 지연 10% 만큼 꾸준히 증가 |
| `[처리]` 체크포인트 | 10초마다 1씩 증가, 실패 0 |
| `[정합성]` 캠페인 표 | imp/req 비율이 0.9 대. **1을 넘는 줄이 있으면 SSAI 열을 볼 것** |
| `[집계]` 그래프 | 파란 선(실시간)이 분마다 갱신 |

**4) 확정 집계를 만든다 — 그래프에 보라색 면이 생긴다**

```bash
make flush && make batch && make recon
```

`make flush` 는 열려 있는 윈도우를 닫는다 (부하가 멈추면 워터마크도 멈추므로 필요).
`make batch` 콘솔에 중복 제거와 SSAI 정리 과정이 단계별로 찍힌다.

→ `[집계]` 그래프에 **보라색 면**이 생기고 `배치 계산 시점` 세로 점선이 그어진다.
→ `[대사]` 패널에 실시간 / 확정 / 차이 / 차이율과 **원인 분해**가 채워진다.

> 파란 선은 계속 이어지고 보라 면은 배치 시점에서 끊긴다.
> "실시간은 계속 흐르고 확정은 주기적으로 따라잡는다" 는 구조가 그림으로 보인다.

**5) 일부러 망가뜨려 본다**

```bash
make scenario-kafka-down
```

가장 극적이다. 브로커를 죽여도 수집은 200 을 준다. 나머지 넷:

```bash
make scenario-tamper       # DLQ 급증 + alert 발화
make scenario-flink-kill   # TaskManager SIGKILL -> 체크포인트 복구
make scenario-burst        # 지연 폭주
make scenario-live         # 2000 EPS + Collector 3대 스케일 아웃
```

시나리오마다 실행 전/중/후 스냅샷을 한 줄씩 찍고 **대시보드의 어느 칸을 보라고 알려 준다.**
자세한 관찰 포인트와 실측값은 §8 참조.

---

### 데이터가 흐르는 경로를 눈으로 따라가기

임프레션 한 건이 지나가는 자리를 순서대로 짚으면 이렇다.

| # | 지점 | 확인 방법 |
|---|---|---|
| 1 | 생성기 | `make load` 콘솔의 `imp` 열 |
| 2 | Collector | http://localhost:8080/actuator/prometheus (`collector_events_accepted_total`) |
| 3 | Kafka | `make consume T=ad.impression N=3` |
| 4 | Flink 중복제거 | 대시보드 `[처리]` 중복제거 (in→out) |
| 5 | Flink 윈도우 | `make consume T=agg.minute N=3` |
| 6 | Redis | `make redis-cli` → `ZREVRANGE agg:index 0 4` → `HGETALL <키>` |
| 7 | MinIO Parquet | http://localhost:9001 → `events` 버킷 → `dt=.../hour=...` |
| 8 | Spark 확정 | `make batch` 콘솔 + `make psql` → `daily_settlement` |
| 9 | 대사 | `make recon` 콘솔 + `make psql` → `reconciliation` |
| 10 | 대시보드 | http://localhost:8088 (1~9 를 한 화면에) |

Redis 를 직접 들여다보는 예:

```bash
docker compose exec redis redis-cli zrevrange agg:index 0 4
```

DLQ 사유별 분포를 세는 예:

```bash
MSYS_NO_PATHCONV=1 docker compose exec -T kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic dlq.invalid --from-beginning --timeout-ms 8000 2>/dev/null | grep -o '"reason":"[^"]*"' | sort | uniq -c | sort -rn
```

---

### "0이면 고장인가?" 판정표

혼동하기 쉬운 부분이라 따로 정리한다.

| 칸 | 0 이어도 정상인 경우 |
|---|---|
| `late.events` / `agg.minute` / Redis | `make flink` 를 안 했다 |
| MinIO `events` 버킷 | `make archive` 를 안 했다 (또는 첫 체크포인트 10초가 안 지났다) |
| `alert.anomaly` | 평상시엔 안 뜬다. `make load ARGS="--drop-impression 0.7"` 또는 `make scenario-tamper` |
| `[대사]` 패널 | `make recon` 을 안 했다 |
| 그래프의 보라색 면 | `make batch` 를 안 했다 |
| `fallback` | 정상이다. Kafka 가 살아 있으면 0 이어야 한다 |
| Outbox 미발행 | 정상이다. 워커가 따라잡고 있으면 0~수십 |
| Flink `입력 건/초` 가 잠깐 0 | Flink REST 메트릭이 10초마다만 갱신된다. 24초 창으로 평균 내지만 초기엔 0 |

---

## 2. 단계 1 — 인프라와 Collector API

### 2-1. 구성 요소

| 서비스 | 이미지 (고정) | 메모리 상한 |
|---|---|---|
| kafka | `apache/kafka:3.9.0` (KRaft 단일 노드) | 1200m |
| redis | `redis:7-alpine` | 256m |
| postgres | `postgres:16-alpine` | 512m |
| minio | `quay.io/minio/minio:RELEASE.2024-09-13T20-26-02Z` | 512m |
| flink jobmanager | `flink:1.20-scala_2.12-java17` | 1200m |
| flink taskmanager | `flink:1.20-scala_2.12-java17` (슬롯 2) | 1700m |
| collector | `ottads/collector:0.1.0` (Kotlin 2.1 / Boot 3.4.1 / JRE 21) | 640m |
| spark | `apache/spark:3.5.3` — `batch` 프로파일, 평소엔 안 뜬다 | 2g |

상시 기동 합계 약 5.9GB. Spark 는 `make batch` 때만 추가로 2GB 를 쓴다.

### 2-2. Collector 엔드포인트

**`POST /v1/events`** — 배치 수신. body 는 JSON 배열, `{"events":[...]}`, 단건 객체 모두 허용.

```json
{"received":3,"accepted":2,"invalid":1,"fallback":0,"tookMs":431}
```

- 필수 필드: `event_id`, `event_type`, `campaign_id`, `event_time`
- `event_time` 은 epoch millis 또는 ISO-8601 → 내부에서 ISO-8601 UTC 로 정규화
- `server_ts` 를 서버가 부여한다. `server_ts - event_time` 이 곧 지연이다.
- 개별 이벤트가 검증 실패해도 **배치 전체를 거절하지 않고** 항상 `202`.
  플레이어 SDK 가 배치 재전송을 반복하면 폭주하기 때문.

**`GET /v1/track`** — VAST 트래킹 픽셀. 항상 `200 image/gif` (1x1).

| 파라미터 | 의미 |
|---|---|
| `eid` / `et` / `cid` / `ts` | event_id / event_type / campaign_id / event_time |
| `arid` | ad_request_id (파티션 키) |
| `crid` / `q` / `sid` | creative_id / quartile / session_id |
| `src` | `client` 또는 `server` (SSAI 이중경로 구분용) |
| `exp` | 만료 epoch seconds |
| `sig` | HMAC-SHA256 서명 (hex) |

서명 규칙:

```
canonical = sig 를 제외한 모든 쿼리 파라미터를 key 오름차순 정렬해 "k=v" 로 만들고 "&" 로 연결
sig       = HMAC-SHA256(COLLECTOR_HMAC_SECRET, canonical) 의 소문자 hex
```

검증 실패(`missing_sig` / `bad_signature` / `expired`)해도 HTTP 는 200 이다.
픽셀이 4xx 를 뱉으면 플레이어가 재시도 폭주를 일으키므로, 실패는 `dlq.invalid` 로만 남긴다.

**`GET /actuator/health`**, **`GET /actuator/prometheus`**

Kafka 는 일부러 헬스 인디케이터에 넣지 않았다. 브로커가 죽어도 Collector 는
fallback 으로 계속 받아야 하므로 `UP` 이어야 한다.

**`GET /v1/admin/fallback`**, **`POST /v1/admin/fallback/replay`** — fallback 파일 상태 조회/재적재.

### 2-3. 토픽

| 토픽 | 파티션(로컬) | 파티션(운영) |
|---|---|---|
| `ad.impression` `ad.quartile` `ad.click` `ad.request` `user.behavior` `dlq.invalid` | 6 | 48 |
| `late.events` `alert.anomaly` | 2 | 12 |

파티션 키는 전부 `ad_request_id` (없으면 `event_id`). 하나의 광고 요청에서 파생된
request→impression→quartile→click 이 같은 파티션에 모여야 Flink 의 상태 연산이
로컬해지기 때문이다.

### 2-4. fail-open 동작

Kafka 발행이 실패하면 예외를 올리지 않고 `/data/fallback/fallback-<YYYYMMDD-HH>.jsonl`
에 한 줄씩 append 한 뒤 **성공(202)으로 응답**한다.

```json
{"topic":"ad.click","key":"<ad_request_id>","payload":"<원본 JSON 문자열>","fallback_ts":"..."}
```

로그에 `FAIL-OPEN` 문자열로 명확히 남는다.

```
FAIL-OPEN kafka publish failed (streak=1) topic=ad.click err=KafkaException: Send failed -> writing to fallback file
FAIL-OPEN fallback file append: total=1 file=/data/fallback/fallback-20260912-08.jsonl topic=ad.click
kafka publish RECOVERED topic=ad.impression
```

유실이 발생할 수 있는 지점은 **파일 쓰기까지 실패했을 때 한 곳뿐**이고,
그때는 `FAIL-OPEN fallback WRITE FAILED (event lost)` 로 ERROR 를 남긴다.

fail-open 이 걸리면 응답이 느려진다. 브로커가 없을 때 프로듀서가 메타데이터를
기다리는 `max.block.ms`(로컬 2초)와 Reactor `publish-timeout-ms`(3초) 중 먼저
걸리는 쪽이 지연 상한이 된다. 실측 약 3초.

### 2-5. 노출 메트릭

```
collector_events_received_total{endpoint="v1_events"|"v1_track"}
collector_events_accepted_total{topic="..."}
collector_events_invalid_total{reason="missing_field"|"track_bad_signature"|...}
collector_dlq_total{reason="..."}
collector_events_fallback_total{topic="..."}
collector_kafka_publish_errors_total{kind="..."}
collector_fallback_pending          # gauge: 아직 재적재 안 된 fallback 건수
```

### 2-6. 확인 방법 (curl → Kafka 콘솔 컨슈머)

`make smoke` 가 아래를 순서대로 수행한다. 수동으로 하려면:

```bash
# (1) 이벤트 3건 넣기 - 마지막 1건은 campaign_id 누락
curl -X POST http://localhost:8080/v1/events -H 'Content-Type: application/json' -d '[
  {"event_id":"e1","event_type":"impression","campaign_id":"cmp-1001","event_time":1789203026484,"ad_request_id":"req-1"},
  {"event_id":"e2","event_type":"quartile","campaign_id":"cmp-1001","event_time":1789203026484,"ad_request_id":"req-1","quartile":"complete"},
  {"event_id":"e3","event_type":"impression","event_time":1789203026484,"ad_request_id":"req-1"}
]'
```

```bash
# (2) Kafka 콘솔 컨슈머로 확인
MSYS_NO_PATHCONV=1 docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic ad.impression --from-beginning --max-messages 5
```

```bash
# (3) 검증 실패분은 dlq.invalid 로
MSYS_NO_PATHCONV=1 docker compose exec kafka \
  /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic dlq.invalid --from-beginning --max-messages 5
```

```bash
# (4) 서명된 트래킹 픽셀 호출
SECRET=local-dev-secret
TS=$(date +%s%3N); EXP=$(( $(date +%s) + 300 ))
CANON="arid=req-1&cid=cmp-1001&eid=px-1&et=impression&exp=$EXP&sid=s1&src=server&ts=$TS"
SIG=$(printf '%s' "$CANON" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')
curl -i "http://localhost:8080/v1/track?eid=px-1&et=impression&cid=cmp-1001&ts=$TS&arid=req-1&sid=s1&src=server&exp=$EXP&sig=$SIG"
```

```bash
# (5) 메트릭
curl -s http://localhost:8080/actuator/prometheus | grep '^collector_'
```

fail-open 확인:

```bash
docker compose stop kafka
curl -X POST http://localhost:8080/v1/events -H 'Content-Type: application/json' \
  -d '[{"event_id":"fo1","event_type":"impression","campaign_id":"cmp-1002","event_time":1789203026484,"ad_request_id":"fo"}]'
cat data/fallback/*.jsonl
curl -s http://localhost:8080/v1/admin/fallback

docker compose start kafka
curl -X POST http://localhost:8080/v1/admin/fallback/replay
```

### 2-7. 지금 무엇이 관찰 가능한가

수집기 한 대가 이벤트를 받아 검증하고 토픽별로 갈라 Kafka 에 넣는 구간 전체가
움직인다. 정상 이벤트가 `event_type` 에 따라 `ad.impression`/`ad.quartile`/`ad.click`
등으로 흩어지고, 필수 필드가 빠지거나 픽셀 서명이 틀린 것은 버려지지 않고
`dlq.invalid` 에 사유(`missing_field:campaign_id`, `track_bad_signature`)와 원본이
함께 쌓이는 것을 콘솔 컨슈머로 직접 확인할 수 있다. 서버가 붙인 `server_ts` 와
이벤트에 실린 `event_time` 이 나란히 남기 때문에, 앞으로 생성기가 지연 이벤트를
섞기 시작하면 이 두 값의 차이만으로 지연 분포를 볼 수 있다. 가장 볼 만한 것은
Kafka 를 정지시켰을 때다. 수집 API 는 여전히 202 를 돌려주고(다만 응답이 3초로
늘어난다), 이벤트는 `data/fallback/*.jsonl` 에 쌓이며, 로그에 `FAIL-OPEN` 이 찍히고
`collector_fallback_pending` 게이지가 올라간다. 브로커를 되살리고 재적재를 호출하면
같은 이벤트가 Kafka 로 다시 흘러가면서 파일이 `.done` 으로 바뀐다 — 이때 발생하는
중복을 뒤 단계의 Flink(1시간 TTL)와 Spark(전체 범위)가 각각 어떻게 다르게
처리하는지가 이 파이프라인에서 볼 만한 지점이 된다.

---

## 3. 단계 2 — 이벤트 생성기 (트래픽 시뮬레이터)

`generator/` 에 Python(asyncio + aiohttp)으로 작성. 상시 기동하지 않고 `load` 프로파일로 필요할 때만 뜬다.

```bash
make load                                   # normal, 200명, 60초
make load MODE=live USERS=600 DURATION=120
make load MODE=burst USERS=300 DURATION=90
make load MODE=normal ARGS="--ssai 0.2 --late 0.4"   # 이상 비율 조절

# Windows
powershell -ExecutionPolicy Bypass -File .\make.ps1 load live 600 120
```

### 3-1. 가상 사용자 모델

사용자 N명이 각자 세션을 갖고 카탈로그(KBO 중계 / 드라마 / 영화 / 예능 / 뉴스 라이브)에서
콘텐츠를 골라 시청한다.

- 시청 중 **30초마다 progress ping** (`user.behavior`)
- 콘텐츠마다 광고 브레이크 지점이 정해져 있다 (프리롤 + 미드롤, live 는 10~15분 간격)
- 브레이크마다 **광고 팟 1~3편**. 광고 1편의 이벤트 시퀀스:
  `ad_request → ad_response → impression → quartile(start,25,50,75,complete) → (10% 클릭)`
- 세션 20% 는 콘텐츠 도중 이탈, 광고 8% 는 재생 중 끊겨 **quartile 이 일부만** 발생
- 하나의 광고에서 나온 모든 이벤트는 같은 `ad_request_id` 를 갖는다 (= Kafka 파티션 키)

> **시간 배율 설계.** 실제 시청 행태를 1배속으로 흉내 내면 사용자 1명이 초당 0.055건
> 정도밖에 만들지 않는다. 2000 EPS 를 보려면 3만 명이 필요하다.
> 그래서 **재생 위치(playhead)만 빨리 감는다.** `event_time` 은 언제나 실제 현재 시각이라
> Flink 워터마크/윈도우는 정상 동작한다. 콘솔의 `x69.4` 가 그 배율이고,
> 목표 EPS 에 맞춰 1초마다 자동 보정된다.
>
> 세션 시작 재생위치는 사용자마다 랜덤이다. 전원이 위치 0에서 출발하면 광고 브레이크가
> 락스텝으로 몰려 "광고 이벤트가 한 번 몰렸다가 한참 없는" 비현실적 파형이 나온다.

### 3-2. 주입하는 이상 케이스

| 인자 | 기본값 | 내용 |
|---|---|---|
| `--late` | 10% | `event_time` 이 30~60초 과거. 네트워크 복구 후 밀린 전송. |
| `--dup` | 5% | 같은 `event_id` 를 0.5~3초 뒤 한 번 더 (다른 배치로) |
| `--bad` | 1% | 필수 필드 하나 제거 → `dlq.invalid` |
| `--forge` | 1% | 픽셀에 엉뚱한 `sig` → `dlq.invalid` |
| `--ssai` | 3% | 같은 impression 이 클라이언트/서버 두 경로로 도착 |

**`dup` 과 `ssai` 를 나눠 둔 이유가 이 실습의 핵심이다.**

- `dup` 은 `event_id` 가 **같다** → Flink 의 `ROW_NUMBER OVER (PARTITION BY event_id)` 로 잡힌다.
- `ssai` 는 `ad_request_id` 는 같지만 `event_id` 가 **다르다** → event_id 중복제거로는 안 잡힌다.
  `source` 필드(`client` / `server`)와 `ssai_twin_of` 로 추적되며,
  단계 5의 Spark 가 `ad_request_id` 단위로 우선순위를 적용해야만 걸러진다.

Kafka 원문에 `_late` / `_dup` / `_bad` / `ssai_twin_of` 표식을 붙여 보내므로,
나중에 "이건 일부러 넣은 이상 케이스였다"를 되짚을 수 있다. 실제 수집기는 모르는 필드를
그대로 통과시키기 때문에 가능한 방법이다.

### 3-3. 부하 모드

| 모드 | 목표 EPS | 내용 |
|---|---|---|
| `normal` | 200 | 평시 |
| `live` | 2000 | KBO 중계 피크 |
| `burst` | 200 | `--burst-period` 30초마다 `--burst-pause` 10초간 **전송만** 정지. 생성은 계속되어 버퍼에 쌓이고, 재개 시 한꺼번에 나간다. |

전송 경로는 두 갈래다. 기본 15% 는 `GET /v1/track` 픽셀(HMAC 서명), 나머지는
`POST /v1/events` 배치(200건/100ms). `--pixel-ratio` 로 조절한다.

### 3-4. 실측 결과

```
normal  300명 30초 : 평균 209 eps, HTTP 오류 0, p95 24ms
live    600명 30초 : 평균 1777 eps (정상구간 ~2000), HTTP 오류 0, p95 83ms
burst   300명 45초 : 정지 10초 동안 버퍼 1938건 적체 → 재개 순간 2106건 일괄 전송, p95 95ms 스파이크
```

콘솔 1초 라인 (실제 출력):

```
t=  21.0s | live   | eps  2243/2000  | tot    40006 | imp  107 qrt   528 clk  12 req  211 beh 1385 |
  late  208 dup 101 bad  17 forge  25 ssai   1 | px  115 | http2xx 2276 err   0 | p95    85ms | x   69.4 | users   600 | buf    136
```

Kafka 에 실제로 쌓인 결과(3회 실행 누계)와 Collector 카운터가 정확히 일치했다.

```
ad.impression  3215    collector_events_received_total{endpoint="v1_events"} 75803
ad.quartile   14889    collector_events_received_total{endpoint="v1_track"}   3544
ad.request     6232    ------------------------------------------------------------
ad.click        309    합계 79347  =  accepted 합계 79347  (fallback 0)
user.behavior 53190
dlq.invalid    1512    missing_field 752 / track_bad_signature 760
```

지연 이벤트 실물 (`server_ts - event_time` = 59초):

```json
{"event_id":"evt-b22658be276642c5","event_type":"impression",
 "event_time":"2026-09-12T09:28:41.012Z","ad_request_id":"req-bb963d22712f42cd",
 "_late":true,"server_ts":"2026-09-12T09:29:40.440585217Z","ingest_endpoint":"v1_events"}
```

SSAI 이중경로 실물 (`source=server`, 원본과 `ad_request_id` 동일 / `event_id` 다름):

```json
{"event_id":"evt-4badc068f31a4a34","event_type":"impression","source":"server",
 "ad_request_id":"req-d557b1290356424e","ssai_twin_of":"evt-f1f3e8c734034712"}
```

### 3-5. 지금 무엇이 관찰 가능한가

파이프라인 앞단이 실제 트래픽 형태로 움직이기 시작했다. 콘솔 1초 라인에서 목표 EPS 를
추종하는 배율(`x`)이 조정되는 과정, 광고 이벤트와 시청 행동 이벤트의 비율(약 37:63)이
안정적으로 유지되는 것, 이상 케이스가 설정한 비율대로 섞여 나가는 것을 볼 수 있다.
Kafka 쪽에서는 `dlq.invalid` 가 `missing_field` 와 `track_bad_signature` 두 사유로
나뉘어 쌓이고, `ad.impression` 안에 `_late` 표식이 붙은 이벤트의 `server_ts - event_time`
차이가 30~60초로 벌어져 있는 것을 직접 꺼내 볼 수 있다. `burst` 모드를 돌리면 버퍼가
0에서 2000 가까이 차올랐다가 한 순간에 비워지는 것이 콘솔의 `buf` 열에 그대로 보이고,
그 순간 Collector 의 응답 p95 가 튀어 오른다. 다만 **지금은 이 지연이 아직 아무 결과도
만들지 않는다** — 지연·중복·SSAI 이중경로를 실제로 "다르게 처리"하는 주체(Flink 워터마크,
Spark 전체범위 중복제거)가 아직 없기 때문이다. `late.events` 와 `alert.anomaly` 토픽이
0건인 것이 그 증거다. 단계 4부터 이 숫자들이 움직이기 시작한다.

---

## 4. 단계 3 — Outbox 경로 (서버 확정 이벤트)

`ad-decision/` 에 Kotlin Spring Boot 앱을 하나 더 둔다. 포트 **8090**.

Collector 와 성격이 정반대라서 스택도 일부러 다르게 골랐다.

|  | Collector | ad-decision |
|---|---|---|
| 스택 | WebFlux (논블로킹) | **Web MVC + JDBC (블로킹)** |
| 실패 시 | 파일에 흘리고 성공 응답 (fail-open) | 플래그를 안 올리고 다음에 재발행 |
| 우선순위 | 가용성 | 정합성 |
| 결과 | 유실 가능 (파일까지 실패할 때) | **중복 발생** (at-least-once) |
| Kafka acks | 1 | all + idempotence |

Outbox 의 핵심은 "소재 결정과 이벤트 INSERT 가 한 트랜잭션" 인 것 하나인데,
R2DBC 반응형 트랜잭션으로 가면 그 핵심이 배관에 묻힌다.

### 4-1. `POST /v1/ad-request`

```bash
curl -X POST http://localhost:8090/v1/ad-request -H 'Content-Type: application/json' \
  -d '{"session_id":"s1","user_id":"u1","content_id":"ct-drama-201","device":"smart_tv","ad_pod_id":"pod-1","ad_slot":0,"playhead_s":900,"max_duration_s":30}'
```

```json
{"ad_request_id":"req-1d02146fb6d34992","fill":true,"campaign_id":"cmp-1003",
 "creative_id":"crt-1003-a","ad_duration_s":15,"decision_ms":1}
```

`@Transactional` 하나 안에서 두 가지가 일어난다.

1. `campaigns` 테이블(60초 캐시)에서 **예산 가중**으로 캠페인을 고른다
2. 같은 트랜잭션에서 `event_outbox` 에 `ad_response` 이벤트를 INSERT

**Kafka 발행은 이 트랜잭션에 넣지 않는다.** DB 커밋과 Kafka 발행은 하나의 원자 단위가
될 수 없기 때문이다. 여기서 발행하면 "커밋은 됐는데 발행 실패" 또는 "발행은 됐는데 롤백"
이 생긴다. 그래서 트랜잭션에는 DB 쓰기만 넣고 발행은 워커가 따로 한다.
그 대가가 중복이다.

**노필(`fill:false`)** 을 기본 8% 넣었다(`AD_NO_FILL_RATE`). 이게 있어야
impression/request 비율이 1 밑으로 내려가고, 단계 4의 이상 감지(임계치 0.5)가 의미를 갖는다.
노필이면 생성기는 impression 이하를 발생시키지 않는다.

### 4-2. Outbox 워커 (같은 앱의 `@Scheduled`)

500ms마다:

1. `published = false` 를 `id` 순으로 최대 500건 조회
2. Kafka `ad.request` 로 **동기** 발행 (파티션 키 = `aggregate_id` = `ad_request_id`)
3. 성공한 id 를 `published = true` 로 업데이트

**3)이 실패하면 그 배치는 다음 폴링에 다시 걸려 재발행된다 = Kafka 에 중복이 생긴다.**
버그가 아니라 Outbox 패턴의 정의된 성질(at-least-once)이고, 이 실습이 관찰하려는 대상이다.
`OUTBOX_UPDATE_FAIL_RATE`(기본 0.02)로 그 상황을 인위적으로 만든다. 0 으로 두면 중복이 사라진다.

로컬은 워커가 1개라 잠금이 없다. 운영에서 인스턴스를 여러 개 띄우면 조회에
`FOR UPDATE SKIP LOCKED` 를 붙여야 한다 (`OutboxWorker.selectSql` 주석 참조).

노출 메트릭:

```
outbox_unpublished              # gauge: 미발행 건수
outbox_lag_seconds              # gauge: 가장 오래된 미발행 행의 나이 = 최고 지연
outbox_published_total          # 발행 시도 누계 (중복 포함)
outbox_republished_total        # 그중 재발행분 = Kafka 에 생긴 중복
outbox_update_skipped_total     # 플래그 업데이트를 못 한 건수
outbox_publish_errors_total{kind}
addecision_requests_total{fill="true"|"false"}
```

`GET /v1/outbox/stats` 로도 같은 값을 JSON 으로 준다 (시나리오 스크립트/대시보드용).

### 4-3. 생성기 연결

`AD_DECISION_URL` 이 설정되면(compose 기본값 `http://ad-decision:8090`) 생성기는
광고 브레이크마다 ad-decision 을 호출한다. 이때 **이벤트 흐름이 두 갈래로 갈라진다.**

```
클라이언트가 본 사실           서버가 확정한 사실
  ad_request                    ad_response
      │                              │
   Collector                    event_outbox (같은 트랜잭션)
      │                              │
      │                        Outbox 워커
      └──────────► ad.request ◄──────┘
```

같은 `ad_request_id` 를 공유하는 두 이벤트가 같은 토픽·같은 파티션에 들어온다.
둘의 개수 차이가 곧 노필/유실/중복이고, 단계 4의 정합성 지표가 이걸 본다.

ad-decision 을 쓰지 않으면(`--ad-decision ""`) 생성기가 예전처럼 스스로 소재를 정하고
`ad_response` 도 직접 낸다.

### 4-4. 실측 결과

생성기 300명 45초 (`event_outbox` truncate 후):

```
생성기      : ad-decision 호출 391  fill 372  nofill 19
DB          : event_outbox 391행, 전부 published=true
메트릭      : outbox_published_total     454
              outbox_republished_total    63
              outbox_update_skipped_total 60
Kafka       : ad.request 안의 outbox 경유 메시지 454건
              그중 고유 event_id 394건  ->  중복 정확히 60건
```

플래그 업데이트 실패가 로그에 그대로 남는다.

```
OUTBOX 플래그 업데이트 실패 재현: 30건 (id 48~77) 을 미발행으로 남긴다 -> 다음 폴링에서 재발행
outbox 재발행 30건 (직전 업데이트 실패분) -> Kafka 에 중복 발생
```

**Kafka 를 정지시켰을 때** (Collector 의 fail-open 과 대비되는 지점):

```
docker compose stop kafka
→ POST /v1/ad-request 는 여전히 200. DB 트랜잭션만 하므로 Kafka 와 무관하다.
→ outbox_unpublished 5, outbox_lag_seconds 10 으로 상승
→ outbox_publish_errors_total{kind="TimeoutException"} 기록
docker compose start kafka
→ 워커가 알아서 배수. unpublished 0, published 454→459. 유실 없음.
```

Collector 는 같은 상황에서 **파일로 흘리고 사람이 재적재를 호출**해야 했다.
ad-decision 은 **DB 에 이미 남아 있어 워커가 알아서 따라잡는다.**
같은 장애에 대한 두 가지 대응을 나란히 볼 수 있는 게 이 단계의 소득이다.

> **jsonb 라운드트립 주의.** `payload` 를 `jsonb` 로 저장하므로 키 순서가 바뀌고
> `": "` 처럼 공백이 들어간다. 파싱에는 영향이 없지만, Kafka 원문을 `grep '"key":"value"'`
> 로 찾을 때 안 걸린다. `grep '"key": "value"'` 로 찾아야 한다.

### 4-5. 지금 무엇이 관찰 가능한가

같은 `ad_request_id` 를 가진 이벤트가 서로 다른 두 경로로 같은 토픽에 도착하는 것을 볼 수
있다. 하나는 플레이어가 보낸 `ad_request` 가 Collector 를 거쳐서, 다른 하나는 서버가
확정한 `ad_response` 가 DB 트랜잭션 → Outbox 워커를 거쳐서 온다. `observe.sh` 의
Outbox 줄에서 미발행 건수와 최고 지연이 0 근처를 유지하다가, Kafka 를 멈추면 즉시 올라가고
되살리면 스스로 0 으로 돌아가는 것을 볼 수 있다. 가장 볼 만한 것은 **중복이 태어나는
순간**이다. 로그에 "플래그 업데이트 실패 재현: 30건" 이 찍히고 1초 뒤 "재발행 30건" 이
따라 나오면, Kafka 안의 outbox 메시지 수(454)와 고유 `event_id` 수(394)가 정확히 그만큼
벌어진다. 이 60건은 **아직 아무도 걷어내지 않은 상태로 토픽에 그대로 있다.** 단계 4의
Flink 가 `event_id` 기준 중복제거로 이걸 잡을 것이고, 같은 시점에 SSAI 이중경로(단계 2에서
심은 것)는 `event_id` 가 달라 **못 잡는다** — 두 종류 중복의 운명이 갈리는 걸 보는 게
다음 단계의 관전 포인트다.

---

## 5. 단계 4 — 실시간 처리 (Flink)

`sql/pipeline.sql` 한 파일. SQL Client 로 실행한다.

```bash
make flink                       # 또는 bash scripts/flink-submit.sh
make flink WM=2                  # 워터마크 지연 2초 -> 지각 이벤트 급증
make flink-cancel
```

SQL Client 는 변수 치환을 못 하므로 `scripts/flink-submit.sh` 가 자리표시자를 `sed` 로
바꾼 뒤 넘긴다. 실제 실행된 SQL 은 `data/flink/pipeline.rendered.sql` 에 남는다.

| 환경변수 | 기본값 | 의미 |
|---|---|---|
| `FLINK_WATERMARK_DELAY` | 10 | 워터마크 허용 지연(초) |
| `SOURCE_IDLE_TIMEOUT` | 5 | 유휴 파티션을 워터마크 계산에서 빼기까지(초) |
| `ALERT_THRESHOLD` | 0.5 | impression/request 경고 임계치 |
| `ALERT_MIN_REQUESTS` | 5 | 표본이 이보다 적으면 경고하지 않음 |
| `STARTUP_MODE` | latest-offset | `earliest-offset` 로 과거분부터 재처리 |

### 5-1. 파이프라인 구조

```
ad.impression ─┐
ad.quartile   ─┤
ad.click      ─┼─► UNION ALL ─► event_id 중복제거 ─► TUMBLE 1분 ─► campaigns lookup join
ad.request    ─┘   (all_events)   (상태 TTL 1h)      (agg_1m)      (PostgreSQL)
   │                                                                     │
   │ CURRENT_WATERMARK() 비교                                            ├─► agg.minute ─► redis-writer ─► Redis
   └──────────────────────────► late.events                              └─► alert.anomaly (ratio < 0.5)
```

**네 소스를 UNION ALL 로 합친 이유**: 로컬 슬롯이 2개뿐이다. 소스마다 중복제거·윈도우를
따로 두면 연산자가 4배가 된다. 합쳐 두면 중복제거 1개, 윈도우 1개로 끝난다.
모든 싱크는 `EXECUTE STATEMENT SET` 으로 **하나의 잡**에 담긴다 (총 8 태스크, 슬롯 1개).

**`ad.request` 의 두 갈래.** 정합성 분모는 서버 확정분만 쓴다.

```sql
WHERE event_type = 'ad_response' AND fill = TRUE AND campaign_id <> 'nofill'
```

클라이언트가 보내는 `ad_request` 는 `campaign_id='pending'` 이라 캠페인별 집계에 못 쓴다.
소재를 정한 건 서버이기 때문이다.

**lookup join 을 윈도우 뒤에 붙인 이유**: 이벤트마다 조인하면 초당 수천 번 DB 를
두드리지만, 분단위 결과에 붙이면 캠페인 수만큼(5회/분)이면 끝난다.

### 5-2. 중복 두 종류의 운명

이 단계에서 가장 볼 만한 부분이다. 격리용 캠페인(`cmp-9999`)으로 확인했다.

**4건을 넣었다.**

| event_id | ad_request_id | source | 성격 |
|---|---|---|---|
| `dupX` | R1 | client | ← 같은 event_id 를 두 번 |
| `dupX` | R1 | client | |
| `ssaiY` | R2 | client | ← SSAI 쌍둥이 (event_id 다름) |
| `ssaiZ` | R2 | server | |

**결과: `impressions = 3`, `ssai_dupes = 1`**

```
campaign_id    cmp-9999
advertiser     테스트광고주            <- PostgreSQL lookup join 결과
campaign_name  중복제거 검증용
impressions    3
ssai_dupes     1
window_start   2026-09-12T10:28:00Z
```

- `dupX` 2건 → **1건으로 접혔다.** `ROW_NUMBER() OVER (PARTITION BY event_id)` 가 잡았다.
- `ssaiY`/`ssaiZ` → **둘 다 살아남았다.** `event_id` 가 다르니 잡힐 수가 없다.

즉 **실시간 집계의 impression 은 SSAI 이중경로만큼 과다 계상되어 있다.**
이걸 걷어내려면 `ad_request_id` 단위로 묶어 `source` 우선순위를 적용해야 하고,
그건 단계 5의 Spark 배치가 한다. 실시간과 확정 집계가 갈라지는 첫 번째 원인이다.

두 번째 원인은 **상태 TTL** 이다. `table.exec.state.ttl = 1h` 이므로 1시간이 지난
`event_id` 는 잊는다. 그 뒤에 도착한 재전송은 실시간에서 중복으로 안 잡힌다.
Spark 는 전체 범위를 보므로 잡는다.

### 5-3. 지각 이벤트

Flink SQL 윈도우는 늦은 레코드를 **조용히 버린다.** DataStream API 의 사이드 아웃풋 같은
장치가 SQL 에는 없다. 그래서 `CURRENT_WATERMARK()` 로 직접 비교해 따로 뽑아낸다.

```sql
INSERT INTO late_events_sink
SELECT ..., CURRENT_WATERMARK(event_time) AS watermark_at,
       TIMESTAMPDIFF(SECOND, event_time, CURRENT_WATERMARK(event_time)) * 1000 AS lateness_ms
FROM impression_raw
WHERE CURRENT_WATERMARK(event_time) IS NOT NULL
  AND event_time < CURRENT_WATERMARK(event_time);
```

실측 (부하 110초, 생성기 지연 주입 10%):

```
late.events 1049건
  ad.quartile   326  (샘플 기준. quartile 이 가장 많으니 지각도 가장 많다)
  ad.impression  56
  ad.click        8
lateness_ms 26000 / 45000 ...  -> 생성기가 심은 30~60초 지연과 일치
```

`lateness_ms` 는 `TIMESTAMPDIFF(SECOND, ...)` 기반이라 **초 단위 해상도**다
(Flink SQL 의 `TIMESTAMPDIFF` 에 밀리초 단위가 없다).

### 5-4. 이상 감지

`impression / request` 비율이 임계치(0.5) 밑으로 떨어지면 `alert.anomaly` 로 경고를 낸다.

평상시 이 비율은 0.9 근처라 경고가 안 뜬다. 발화시키려면 "서버는 광고를 채웠는데
플레이어에서 렌더가 안 되는" 상황이 필요하고, 생성기에 `--drop-impression` 노브를 넣었다.

```bash
bash scripts/load.sh --drop-impression 0.7
```

```
10:22 cmp-1003  imp=43  req=146  ratio=0.295  (임계 0.5)  low_impression_ratio
10:23 cmp-1002  imp=13  req=76   ratio=0.171  (임계 0.5)  low_impression_ratio
10:23 cmp-1003  imp=65  req=222  ratio=0.293  (임계 0.5)  low_impression_ratio
```

### 5-5. 체크포인트

10초 간격, 파일시스템(`/data/checkpoints`).

```
완료 31  실패 0  진행중 0
최근: id=31  크기=438.0KB  소요=26ms
경로: file:/data/checkpoints/9408a45bf7efed232952fab64028b9d0/chk-31
```

상태 백엔드는 `hashmap`(힙) + 체크포인트 저장소 `filesystem` 이다.
초당 2000건을 한 시간 넘게 돌리면 중복제거 상태(1시간 TTL)가 힙을 압박한다.
그때는 `.env` 에 `FLINK_STATE_BACKEND=rocksdb` 를 넣는다 (디스크 기반, 지연은 늘어남).

### 5-6. Redis 적재 — 그리고 왜 중간 단계가 하나 더 있는가

> **여기가 이번 단계에서 원안대로 못 한 유일한 부분이다.**
>
> `Flink → Redis` 를 SQL 로 직접 쓰려 했으나 **Flink 1.20 용 Redis SQL(Table) 커넥터가
> 존재하지 않는다.** Maven Central 을 뒤진 결과:
>
> | 후보 | 상태 |
> |---|---|
> | Apache 공식 | 없음 |
> | `org.apache.bahir:flink-connector-redis 1.1.0` | Flink 1.1 시절. Bahir 프로젝트 자체가 은퇴 |
> | `io.github.jeff-zou:flink-connector-redis 1.4.3` | pom 의 `flink.version = 1.15.1` |
> | `com.redis:redis-flink-connector 0.0.9` | `flink-core 1.19` 기반 **DataStream** 싱크. Table/SQL 팩토리 없음 |
>
> 임의로 버전을 올려 끼우면 Table API 내부 변경 때문에 런타임에 깨진다.
> 그래서 **Flink 는 Kafka(`agg.minute`)까지만 내보내고, `redis-writer` 컨테이너가
> Redis 에 적재한다.** 실제 운영에서도 "스트림 → Kafka → 서빙 스토어" 는 흔한 구성이라
> 아키텍처가 크게 어긋나지는 않지만, 원안과 다른 지점이므로 명시해 둔다.

Redis 키 구조 (전부 TTL 48시간):

```
agg:1m:<campaign_id>:<yyyymmddHHMM>   HASH   impressions/clicks/completes/requests/
                                             imp_req_ratio/ssai_dupes/advertiser/campaign_name
agg:campaigns                          SET    등장한 campaign_id
agg:index                              ZSET   score=window_start epoch, member=위 키
late:1m:<yyyymmddHHMM>                 STRING 지각 이벤트 카운터
late:total                             STRING 누계
alerts:recent                          LIST   최근 100건 (JSON)
alerts:total                           STRING 누계
```

```bash
docker compose exec redis redis-cli zrevrange agg:index 0 4
docker compose exec redis redis-cli hgetall agg:1m:cmp-1003:202609121023
docker compose exec redis redis-cli lindex alerts:recent 0
docker compose exec redis redis-cli get late:total
```

### 5-7. 겪은 문제 — 유휴 파티션이 워터마크를 붙잡는다

처음 제출했을 때 잡은 `RUNNING`, 예외 0, 이벤트는 계속 들어오는데 **`agg.minute` 이
한 건도 안 나왔다.**

원인: 워터마크는 모든 입력(4개 토픽 × 6파티션 = 24개)의 **최솟값**이다.
`ad.click` 은 100초에 30건 정도라 6개 파티션 중 몇 개는 한동안 비어 있는데,
그 파티션의 워터마크가 과거에 머물면서 전체 워터마크를 붙잡는다. 윈도우가 영원히 안 닫힌다.

```sql
SET 'table.exec.source.idle-timeout' = '5 s';
```

이 한 줄로 해결됐다. Flink 로 여러 토픽을 합칠 때 가장 흔히 밟는 지뢰라
`SOURCE_IDLE_TIMEOUT` 으로 조절할 수 있게 빼 두었다 (0 으로 두면 다시 재현된다).

### 5-8. 지금 무엇이 관찰 가능한가

지금까지 0 이던 세 곳이 동시에 움직이기 시작한다. `late.events` 에는 생성기가 심은
30~60초 지연 이벤트가 `lateness_ms` 와 함께 쌓이고, `alert.anomaly` 에는 impression 이
말라붙은 캠페인이 비율과 함께 올라오며, Redis 에는 분단위 집계 해시가 48시간 TTL 을
달고 생긴다. `observe.sh` 에 Flink 잡 상태와 Redis 키 개수 줄이 추가됐으니 한 화면에서
같이 볼 수 있다. Flink UI(http://localhost:8181)에서는 8개 태스크가 슬롯 1개에 얹혀
돌아가는 것과 10초마다 체크포인트가 찍히는 것을 볼 수 있다.

가장 볼 만한 것은 **같은 "중복"인데 결과가 다르다**는 점이다. 같은 `event_id` 재전송은
윈도우에 들어가기 전에 접히지만, SSAI 이중경로는 `event_id` 가 달라 그대로 통과해
`ssai_dupes` 로 세어질 뿐 impression 에서 빠지지 않는다. **지금 Redis 에 있는 실시간
impression 값은 SSAI 만큼 부풀어 있다.** 여기에 상태 TTL 1시간을 넘겨 도착한 재전송까지
더하면, 실시간 집계와 앞으로 만들 확정 집계의 차이가 어디서 오는지가 이미 정해진 셈이다.
단계 5의 Spark 가 전체 범위 중복제거와 `ad_request_id` 단위 SSAI 처리로 그 차이를
메우고, 대사(reconciliation) 잡이 차이율과 원인을 표로 뽑는다.

---

## 6. 단계 5 — 원본 적재와 배치 확정

```bash
make archive                 # MinIO Parquet 적재 잡 제출 (Flink 두 번째 잡)
make load                    # 부하
make flush                   # 열려 있는 윈도우 닫기  <- 로컬에서만 필요. 아래 6-5 참조
make batch                   # Spark 정산 배치
make recon                   # 대사
```

### 6-1. 원본 Parquet 적재 (`sql/archive.sql`)

Kafka Connect 대신 **Flink 파일 싱크**를 골랐다.

1. Confluent S3 Sink 는 Confluent Community License 라 배포 조건이 다르다
2. Connect 워커 컨테이너가 하나 더 필요하다 (로컬 8GB 예산에 부담)
3. Flink 는 이미 떠 있고, **체크포인트에 맞춰 파일을 커밋**하므로 exactly-once 파일 커밋이 공짜다

```
s3a://events/dt=2026-09-12/hour=11/part-<uuid>-0-0
```

다섯 토픽(`ad.impression` `ad.quartile` `ad.click` `ad.request` `user.behavior`)을
공통 스키마로 합쳐 **가공 없이** 넣는다. 필터도, 중복 제거도, 집계도 없다.
지연·중복·SSAI 이중경로가 전부 그대로 들어간다. 걷어내는 건 배치 몫이다.

**파일이 보이기까지 걸리는 시간 = 다음 체크포인트까지(최대 10초).**
Parquet 은 bulk 포맷이라 롤링 간격(`ROLLOVER`, 기본 1분)과 상관없이 **체크포인트마다 파일을 닫는다.**
그래서 파일은 빨리 보이지만 잘게 쪼개진다 (실측 파일당 수 KB — [docs/08-minio-guide.md](docs/08-minio-guide.md) 4절).
(처음에는 "롤링 1분 + 체크포인트 = 최대 70초" 로 적었으나, MinIO trace 로 10초 간격 생성을 확인하고 고쳤다.)

슬롯 예산: 이 잡이 1개, `pipeline.sql` 이 1개 → **로컬 2슬롯을 꽉 채운다.**

```
ott-ads-realtime     running=8/8
ott-ads-archive      running=10/10
slots-available: 0
```

### 6-2. 확정 집계 (`spark/batch_settlement.py`)

MinIO Parquet 전체를 읽어 실시간이 못 한 두 가지를 한다.

```
1) event_id 기준 전체 범위 중복 제거
   Flink 는 상태 TTL 1시간 안에서만 본다. 배치는 전체를 본다.

2) SSAI 이중경로 처리
   같은 ad_request_id 에 impression 이 둘 이상이면 source 우선순위로 하나만 채택.
   기본 server > client (SSAI_SOURCE_PRIORITY 로 변경).
   서버 비콘이 광고차단의 영향을 안 받아 신뢰도가 높기 때문.
```

실측:

```
총 행수      : 25,266
중복 제거 후 : 24,161   (제거 1,105건 = 4.37%)
SSAI 제거 전 : 886 impression -> 채택 856  (버린 이중경로 30)
채택된 것의 source 분포: client=828, server=28
```

```
dt         campaign       req    imp  raw_imp  ssai   dup  late  clk  cmpl       cpm      amount
2026-09-12 cmp-1001       195    188      194     6     7    17   16   179     9,500    1,786.00
2026-09-12 cmp-1002       135    133      139     6     2    14   10   119    12,000    1,596.00
2026-09-12 cmp-1003       381    377      391    14    15    28   31   337    15,000    5,655.00
2026-09-12 cmp-1004        96     96       98     2     2     4    6    89     7,000      672.00
2026-09-12 cmp-1005        65     62       64     2     2     5    7    58     6,000      372.00
                                                                              합계   10,081.00 KRW
```

- `raw_imp` = event_id 중복 제거 후 / SSAI 제거 전 → **실시간(Flink)이 세는 값과 같은 지점**
- `imp` = 거기서 SSAI 까지 정리한 **확정값** (정산 기준)
- 금액 = `impressions / 1000 × cpm`

`daily_settlement` 은 매번 truncate 후 전체 재적재한다. 배치가 항상 전 범위를 다시
계산하므로 몇 번 돌려도 결과가 같다 (멱등).

### 6-3. 대사 (`spark/reconcile.py`)

Redis 실시간 합계와 PostgreSQL 확정 집계를 캠페인별로 비교하고 **차이를 원인별로 분해**한다.

```
확정 = 실시간 + 지연반영 − SSAI이중경로 (+ 잔차)
```

Redis 는 `redis-writer` 의 `GET /agg/summary?dt=` 로 읽는다 (Spark 컨테이너에 redis
클라이언트를 넣지 않으려고 HTTP 로 뺐다. 대시보드도 같은 엔드포인트를 쓴다).

실측:

```
campaign        실시간      확정     차이      차이율   원인 분해
cmp-1001        184     188      4   2.13%   지연반영 +17 , SSAI이중경로 -6 , 잔차 -7
cmp-1002        134     133     -1  -0.75%   지연반영 +14 , SSAI이중경로 -6 , 잔차 -9
cmp-1003        376     377      1   0.27%   지연반영 +28 , SSAI이중경로 -14 , 잔차 -13
cmp-1004         96      96      0   0.00%   지연반영 +4 , SSAI이중경로 -2 , 잔차 -2
cmp-1005         61      62      1   1.61%   지연반영 +5 , SSAI이중경로 -2 , 잔차 -2
합계            852     859      7   0.81%   지연반영 +68 , SSAI이중경로 -30 , 잔차 -31
```

**잔차가 음수인 이유**도 설명된다. `_late` 표식이 붙은 68건 중 31건은 실제로는
윈도우가 닫히기 전에 도착해 **실시간에도 이미 반영**됐다. 즉 "지연 주입 = 실시간 누락"
이 아니라, 워터마크와 윈도우 경계의 타이밍에 따라 갈린다.

결과는 `reconciliation` 테이블에 append 된다 (실행 이력이 쌓인다).

```bash
docker compose exec postgres psql -U ads -d adplatform \
  -c "select campaign_id, realtime_impressions, batch_impressions, diff, diff_rate, likely_cause
      from reconciliation order by run_at desc limit 10;"
```

### 6-4. 겪은 문제 — Flink SQL Client 의 후행 주석

`archive.sql` 을 처음 제출했을 때:

```
java.lang.IllegalArgumentException: only single statement supported
```

원인: SQL Client 의 `-f` 모드는 파일을 `;` 로 잘라 조각별로 파싱한다.
`;` **뒤에 같은 줄로 주석**을 달면 그 주석이 다음 조각 앞에 붙어 파서가 "문장 2개" 로 본다.

```sql
SET 'parallelism.default' = '1';   -- 이러면 죽는다
```

```sql
-- 이렇게 위에 쓴다
SET 'parallelism.default' = '1';
```

### 6-5. 겪은 문제 — 부하가 멈추면 마지막 윈도우가 안 닫힌다

첫 대사에서 차이율이 **+37%**, 잔차 +282 로 나왔다.

Flink 이벤트타임 윈도우는 **워터마크가 윈도우 끝을 지날 때** 발화하고, 워터마크는
들어오는 이벤트의 `event_time` 에서 나온다. 부하를 멈추면 워터마크도 멈춰서
마지막 1~2분 윈도우가 영원히 안 닫힌다 → Redis 실시간 합계에 그 구간이 통째로 빠진다.

운영에서는 트래픽이 끊기지 않으니 저절로 해결된다. 로컬에서 "부하 → 배치 → 대사" 를
한 번에 돌릴 때만 생기는 문제라 `scripts/flush-windows.sh` 를 두었다.
검증용 캠페인(`cmp-9999`)으로 소량 이벤트를 25초 간격으로 3번 보내 워터마크를 민다.

```
flush 전 : 차이율 +37.38%  잔차 +282
flush 후 : 차이율  +0.81%  잔차  -31
```

### 6-6. 지금 무엇이 관찰 가능한가

파이프라인이 처음으로 **한 바퀴 닫혔다.** 같은 이벤트가 두 경로로 흘러 서로 다른 답을
내놓고, 그 차이가 왜 생겼는지까지 숫자로 분해된다.

MinIO 콘솔(http://localhost:9001)에서 `events/dt=.../hour=.../` 아래 Parquet 이 체크포인트
주기에 맞춰 쌓이는 걸 볼 수 있고, `make batch` 를 돌리면 전체 25,266행에서 중복 1,105건이
접히고 SSAI 이중경로 30건이 정리되는 과정이 단계별로 찍힌다. 그 뒤 `make recon` 이
실시간 852 vs 확정 859 를 나란히 놓고 "지연반영 +68, SSAI -30, 잔차 -31" 로 쪼갠다.

**여기서 배울 게 하나 있다.** 흔히 "실시간은 대략, 배치가 정답" 이라고 뭉뚱그리는데,
실제로는 방향이 양쪽이다. 지연 이벤트는 실시간을 **과소** 계상하게 만들고(배치가 더 큼),
SSAI 이중경로와 장기 중복은 실시간을 **과대** 계상하게 만든다(배치가 더 작음).
두 힘이 상쇄되면 총합 차이가 작아 보여도 캠페인별로는 부호가 갈릴 수 있다.
위 표에서 cmp-1002 만 확정이 더 작은 게 그 예다. 총합만 보고 "정합하다" 고 하면
안 되는 이유가 이 표에 그대로 있다.

---

## 7. 단계 6 — 관찰 대시보드

**http://localhost:8088** — `make up` 하면 같이 뜬다.

```
dashboard/
  index.html   단일 HTML + 바닐라 JS. 프레임워크 없음. 1초 폴링. canvas 그래프.
  api.py       FastAPI. 여섯 군데를 긁어 /api/overview 하나로 합쳐 준다.
```

### 7-1. API 가 긁어 오는 곳

| 출처 | 무엇 |
|---|---|
| Collector `/actuator/prometheus` | 수신/검증실패/fallback/DLQ 카운터 |
| ad-decision `/actuator/prometheus` | Outbox 미발행·최고지연·재발행, 노필 비율 |
| Kafka Admin API | 토픽별 produce 누계, 컨슈머 그룹 랙 |
| Flink REST `/jobs/<id>` | 연산자별 read/write records, 체크포인트 |
| Redis | 실시간 분단위 집계, 최근 alert |
| PostgreSQL | 확정 분단위/일단위 집계, 대사 결과, Outbox 행수 |

브라우저가 1초마다 폴링하는데 매 요청마다 여섯 군데를 찌르면 응답이 1초를 넘긴다.
그래서 **백그라운드 샘플러가 1초 주기로 스냅샷을 갱신하고 HTTP 핸들러는 그것만
돌려준다** (Kafka 랙은 비싸서 3틱마다).

### 7-2. 화면 구성

```
[수집]    초당 수신 431/s · 누적 205,361 · 검증실패 3,899 · fallback 0(미재적재 0) · DLQ 3,899
          missing_field 1,922   track_bad_signature 1,977
[버퍼]    토픽별 produce 누계 + 건/초 막대 + consumer lag (flink-rt / flink-archive / redis-writer)
[처리]    입력 172/s · 누계 19,891 · 중복제거 661 (16,662→16,001) · late.events 1,426 · 체크포인트 72(실패 0)
[정합성]  Outbox 미발행·최고지연·재발행 / 노필 비율 / 캠페인별 imp·req·비율·SSAI
[집계]    분 단위 impression 그래프 — 실시간(파랑 선) vs 확정(보라 면)
[대사]    실시간 합계 vs 확정 합계 · 차이 · 차이율 · 정산 금액 + 캠페인별 원인 분해
[경고]    alert.anomaly 최근 10건
```

### 7-3. 그래프 — 실시간 vs 확정을 겹쳐 보기

이 프로젝트에서 가장 볼 만한 화면이다.

- **파란 선** = Redis 의 분 단위 실시간 집계 (Flink 산출). 지금 이 순간까지 이어진다.
- **보라 면** = PostgreSQL `minute_settlement` (Spark 배치 산출). **배치를 돌린 시점에서 끊긴다.**
- 끊기는 지점에 `배치 계산 시점` 세로 점선이 그려진다.

두 값이 같으면 파란 선이 보라 면 위에 정확히 겹쳐 보이고, 갈라지면 눈으로 바로 보인다.
확정선을 0 으로 이어 그리지 않고 **끊는** 이유는, 배치를 안 돌린 구간을 "확정 0" 으로
오해하지 않게 하기 위해서다.

> 확정 선이 아예 없으면 `make batch` 를 아직 안 돌린 것이다.
> 배치는 `minute_settlement` 테이블도 함께 채운다 (단계 5에서 추가).

### 7-4. Flink 연산자별 계수를 보려고 체이닝을 껐다

`[처리]` 패널의 **중복제거 661 (16,662→16,001)** 은 Flink 의 `Deduplicate` 연산자
하나의 `read-records` / `write-records` 다. 기본 설정이면 8개 연산자가 하나의 vertex 로
체이닝되어 이 값을 따로 볼 수 없다.

```sql
-- sql/pipeline.sql
SET 'pipeline.operator-chaining' = 'false';   -- FLINK_OPERATOR_CHAINING 으로 변경
```

관찰이 목적이라 기본을 `false` 로 두었다. 대신 연산자 사이 직렬화 비용이 조금 들고
태스크 수가 8 → 34 로 늘어난다 (병렬도 1이라 슬롯은 여전히 1개).

체이닝을 켜고 싶으면 `FLINK_OPERATOR_CHAINING=true make flink`. 그러면 `[처리]` 패널의
중복제거 칸이 0 으로 나온다.

### 7-5. 겪은 문제 — 1초 폴링인데 Flink 지표가 0으로 찍힌다

처음엔 `입력 건/초` 가 계속 0 이었다. Flink REST 의 `read-records`/`write-records` 는
`metrics.fetcher.update-interval`(기본 10초)마다만 갱신된다. 1초 간격으로 차분을 내면
열 번 중 아홉 번은 값이 그대로라 0 이 나온다.

증가율을 **8초 시간창 평균**으로 바꿔 해결했다 (`RATE_WINDOW`).

### 7-6. CLI 대체 수단

브라우저를 안 띄우고 싶으면 `bash scripts/observe.sh` 가 같은 내용을 2초 간격
텍스트로 보여 준다 (그래프와 대사 표는 없다).

### 7-7. 지금 무엇이 관찰 가능한가

지금까지 로그와 `psql`, 콘솔 컨슈머로 하나씩 확인하던 것이 **한 화면에서 동시에 움직인다.**
`make load` 를 걸어 두고 대시보드를 보면, 수집 카운터가 초당 400건대로 올라가는 것과
거의 동시에 Kafka 토픽 막대가 자라고, 그 뒤를 Flink 입력 건/초가 따라오며,
중복제거 칸의 `16,662→16,001` 이 벌어지는 것이 보인다. `late.events` 는 생성기가 심은
지연 비율만큼 꾸준히 증가하고, 체크포인트는 10초마다 하나씩 늘어난다.

정합성 패널에서 `cmp-1005` 의 imp/req 비율이 **1.008** 로 1을 넘는 순간이 있는데,
이게 SSAI 이중경로가 실시간 집계를 부풀린 흔적이다. 같은 행의 SSAI 열이 그 원인을 가리킨다.

그리고 `make batch` 를 돌리면 그래프에 보라색 확정 면이 채워지고 `배치 계산 시점` 선이
그어진다. 그 뒤로 파란 실시간 선만 홀로 이어지는 모습이 "실시간은 계속 흐르고 확정은
주기적으로 따라잡는다" 는 람다 아키텍처의 구조를 그대로 보여 준다.

---

## 7-8. 이벤트 추적기 (http://localhost:3000)

대시보드가 "전체가 얼마나 흐르나" 라면, 추적기는 **"한 건이 어디까지 갔나"** 다.
버튼을 누르면 이벤트를 만들어 넣고, 그 `event_id` 가 다섯 자리에 언제 나타나는지
1초마다 확인해 보여 준다.

```
1. Collector 수신        HTTP 응답 (accepted / invalid / fallback)
2. Kafka 토픽 도착        어느 토픽 / 파티션 / 오프셋
3. Flink -> Redis 집계    중복제거 + 1분 윈도우를 통과해 몇 건으로 반영됐나
4. MinIO Parquet 원본     가공 없는 원본에 몇 행으로 적재됐나
5. PostgreSQL 확정 집계   Spark 배치 결과
```

### 버튼 7개

| 버튼 | 무엇을 보여 주나 |
|---|---|
| 정상 임프레션 1건 | 기준선. 다섯 단계를 전부 통과한다 |
| **중복 — 같은 event_id 2번** | Kafka 2건 → **Redis +1** (Flink 가 접음) |
| **SSAI 이중경로 — event_id 다름** | Kafka 2건 → **Redis +2** (못 접음!) |
| 지연 이벤트 — 60초 과거 | `late.events` 로 빠지고 실시간 집계에는 +0 |
| 스키마 위반 — campaign_id 누락 | `dlq.invalid` 로. 응답은 202 |
| 서명 위조 트래킹 픽셀 | `dlq.invalid` 로. 응답은 200 + 1x1 GIF |
| 광고 요청 — Outbox 경로 | Collector 를 안 거치고 DB → 워커 → Kafka |

### 실측 — 이 두 줄이 이 프로젝트의 핵심이다

| 클릭 | Kafka | Redis(실시간) | Parquet(원본) | 확정(배치) |
|---|---|---|---|---|
| 중복 (같은 `event_id`) | 2건 | **+1** | 2행 | 1 (`dupes_removed` 1) |
| SSAI (`event_id` 다름) | 2건 | **+2** | 2행 | 1 (`raw_impressions` 2 → 1) |

**같은 "2건" 인데 실시간 결과가 다르다.** 중복은 Flink 의 `event_id` 중복제거에 걸려
바로 접히고, SSAI 는 `event_id` 가 달라 그대로 통과해 실시간 집계를 부풀린다.
`ad_request_id` 로 묶는 Spark 배치에 가서야 정리된다.
지금까지 문서로만 설명하던 것을 **버튼 두 번으로 비교**할 수 있다.

### 구현에서 짚어둘 두 가지

**추적마다 캠페인을 새로 판다** (`cmp-tr-<trace_id>`).
처음에는 `cmp-trace` 하나를 공유했는데, 같은 분 윈도우에 두 번 누르면 Redis 해시를
공유해 서로의 건수를 같이 세어 버렸다 (중복도 +3, SSAI 도 +3). 비교가 성립하지 않는다.

**워터마크를 자동으로 민다.**
Flink 윈도우는 워터마크가 윈도우 끝을 지나야 발화하고, 워터마크는 들어오는 이벤트의
`event_time` 에서 나온다. 추적기만 쓰는 상황(생성기 부하 없음)에서는 내가 넣은 1건 뒤로
아무것도 안 와서 윈도우가 영원히 안 닫힌다. 그래서 30초 / 65초 뒤에 별도 캠페인
(`cmp-9999`)으로 소량 이벤트를 흘려 워터마크를 민다. 운영에서는 트래픽이 안 끊기므로
필요 없는 장치다 (§6-5 와 같은 이유).

Parquet 조회는 pyarrow 의 S3FileSystem 으로 해당 `dt/hour` 파티션만 읽어
`event_id` 로 필터한다. 전체를 훑지 않는다.

Kafka 스캔은 전송 직전의 오프셋을 기억해 그 지점부터만 훑는다.
부하가 도는 중에도 토픽 전체를 다시 읽지 않는다.

---

## 7-9. 단계 8 — 플레이어 화면 (http://localhost:3001)

추적기(:3000)가 "이벤트 1건을 골라 넣는 실험대" 라면, 이쪽은 **사용자 화면**이다.
OTT 앱의 재생 화면에 준하는 UI 를 띄우고 거기서 일어나는 일을 진짜 파이프라인에 그대로 흘린다.

**동영상 파일은 없다.** 재생 중인 것처럼 보이는 것은 CSS 로 그린 장면과 재생위치 타이머뿐이다.
그 밖의 모든 것 — 광고 결정 호출, 비콘 전송, 토픽 적재 — 은 실제 동작이다.

### 화면 구성

| 영역 | 내용 |
|---|---|
| 홈 | 콘텐츠 5편(생성기와 같은 `content_id`) + **집행 중인 캠페인 목록** (`GET /v1/campaigns`) |
| 재생 화면 | 16:9 스테이지, 타임라인에 광고 브레이크 마커, 배속(1x/10x/60x/180x), [다음 광고 브레이크로] |
| **광고 오버레이** | 광고주 · 캠페인명 · 카피 · CTA 버튼 · 건너뛰기 카운트다운, 하단에 `campaign_id` / `creative_id` / 업종 / CPM / 결정 지연 / `ad_request_id` |
| 이벤트 로그 | 방금 보낸 이벤트마다 **어느 토픽으로 갔는지 / 배치인지 픽셀인지 / HTTP 응답** |
| 이상 주입 | 중복 재전송, SSAI 이중경로, 60초 지연, 스키마 위반, 서명 위조 픽셀, 픽셀 전송 on/off |
| 데이터 확인 | 방금 그 광고 1편이 Kafka → Redis → MinIO → PostgreSQL 어디까지 갔는지 |

### 주소 (해시 라우팅)

화면 상태를 URL 에 둔다. 뒤로가기·새로고침·링크 공유가 그대로 동작한다.

| 주소 | 화면 |
|---|---|
| `http://localhost:3001/#/` | 홈 (콘텐츠 목록 + 집행 중인 캠페인) |
| `http://localhost:3001/#/watch/ct-drama-201` | 그 콘텐츠 재생 화면 (딥링크로 바로 열린다) |
| `http://localhost:3001/#/track/req-play-xxxxxxxx` | 광고 1편 추적 단독 화면 (3초 자동 갱신, 공유 가능) |

재생 화면을 벗어나면 `session_end` 가 나가고 세션이 닫힌다 — 실제 플레이어와 같다.
그래서 추적 단독 화면은 [단독 화면 ↗ 새 탭] 으로 **새 탭에서** 연다 (보던 재생이 안 끊기게).

### 왜 이 화면을 추가했나

기존 화면들은 **집계된 숫자**만 보여 줬다. "어떤 광고가 나갔는지" 가 어디에도 없어서,
쌓이는 데이터가 무엇에서 나온 것인지 연결이 안 됐다.
플레이어는 그 연결고리다 — 화면에서 본 광고의 `campaign_id` 를 그대로 들고
대시보드·Redis·Parquet·정산 테이블에서 같은 값을 찾을 수 있다.

### 구현에서 짚어둘 네 가지

**콘텐츠는 빨리 감고, 광고는 실제 시간으로 재생한다.**
콘텐츠를 실시간으로 보면 광고 브레이크까지 15분을 기다려야 한다(기본 60배속 → 15초).
반대로 광고까지 빨리 감으면 비콘 간격이 찌그러져 Flink 윈도우/워터마크가 의미를 잃는다.
그래서 광고 구간만 실제 시간이다. 생성기의 `Clock.speed` 와 같은 발상이다.

**비콘은 서버가 대신 쏜다.**
`GET /v1/track` 은 HMAC 서명이 필요한데, 브라우저에서 서명하려면 비밀키를 화면에 내려야 한다
(그러면 위조가 자유로워진다). Collector 에 CORS 도 열어야 한다.
실제 플레이어에서는 광고 서버가 **서명된 URL** 을 VAST 응답에 담아 주고 플레이어는 그대로 호출한다 —
어느 쪽이든 서명은 서버가 만든다는 점이 같다.

**광고 화면은 DOM 을 다시 그리지 않는다.**
카운트다운·진행바 때문에 100ms 마다 갱신이 필요한데, `innerHTML` 을 통째로 갈아 끼우면
mousedown 과 mouseup 사이에 버튼이 교체되어 **click 이벤트가 성립하지 않는다**
(건너뛰기/CTA 가 안 눌리던 원인). 그래서 광고 DOM 은 `mountAd()` 로 한 번만 만들고
`updateAd()` 가 텍스트·너비·disabled 만 바꾼다.

**추적용 Kafka 오프셋은 3초 주기로 미리 떠 둔다.**
광고 요청마다 7개 토픽 × 파티션의 끝 오프셋을 조회하면 광고 시작이 눈에 띄게 느려진다.
백그라운드에서 스냅샷을 갱신해 두고 그 복사본을 쓴다. 최대 3초 앞에서부터 스캔하지만
`event_id` / `ad_request_id` 로 거르므로 결과는 같다.

### 실측 (브라우저에서 광고 1편 재생)

```
1. 플레이어 → Collector   ad_request / impression / quartile×5   HTTP 202, 픽셀 200
2. Kafka 토픽             +3s  ad.impression p5 @749
                          +3s  ad.request    p5 @1561  (ad_response, Outbox 경유)
                          +3s  ad.request    p5 @1562  (ad_request, Collector 경유)
                          +4.2s ~ +15.3s     ad.quartile p5 @3386~3389
3. Flink → Redis          agg:1m:cmp-1001:202609180707  impressions 1, requests 1, ratio 1.0
4. MinIO Parquet          8행 (ad_request, ad_response, impression, quartile×5)
                          s3a://events/dt=2026-09-18/hour=07/
5. PostgreSQL             event_outbox #4557 published=true
                          daily_settlement 은 배치를 돌려야 나온다
```

같은 광고 1편이 **같은 파티션(p5)** 에 모여 있는 것이 파티션 키 설계(`ad_request_id`)의 결과다.

---

## 8. 단계 7 — 시나리오 스크립트

각 상황을 재현하고 **대시보드에서 무엇이 보이는지**를 스크립트가 직접 알려 준다.
모든 시나리오는 실행 전/중/후 스냅샷을 한 줄씩 찍는다 (대시보드 API 를 그대로 재사용).

```bash
make scenario-live          # 또는 bash scripts/scenario_live.sh
make scenario-kafka-down
make scenario-flink-kill
make scenario-burst
make scenario-tamper
```

> 전제: `make up` → `make flink` → `make archive` 가 먼저 되어 있어야 한다.
> 브라우저에 **http://localhost:8088** 을 띄워 놓고 돌리는 걸 권한다.

스냅샷 한 줄의 형식:

```
  시점         수신 (초당)  검증실패  fallback  DLQ | Flink 입력/중복제거/late | Outbox | Redis late/alert | 인스턴스
```

---

### 8-1. `scenario_live.sh` — 라이브 피크 + Collector 수동 스케일 아웃

Kubernetes HPA 가 없으므로 `docker compose up -d --scale collector=3` 으로 수동 스케일 아웃한다.
그래서 collector 에는 `container_name` 을 두지 않았고, 포트를 범위(`8080-8085`)로 열었다.
(그 대가로 **Flink UI 를 8081 → 8181 로 옮겼다.** 8081 이 collector 2번째 인스턴스와 겹치기 때문.)
컨테이너끼리는 Docker DNS 라운드로빈으로 `collector:8080` 을 나눠 쓴다.

**실측**

| 시점 | 수신 누계 | 인스턴스 |
|---|---|---|
| 평시 종료 (200 EPS, 30초) | 6,254 | 1 |
| 스케일 아웃 직후 | 6,254 | 3 |
| 피크 종료 (2000 EPS, 60초) | 123,816 | 3 |

생성기 기준 **117,562 이벤트 / 62.9초 = 평균 1,868 EPS, HTTP 오류 0**.

**대시보드에서 볼 것**

- `[수집]` 제목 옆 `인스턴스 3/3`. 초당 수신이 200 → 1,900 대로.
- `[버퍼]` 토픽 막대가 전부 자람. `user.behavior` 가 압도적.
- `[처리]` Flink 입력 건/초가 뒤따라 오름 (수집보다 느리게 따라온다).
- `consumer lag` 이 벌어졌다가 좁혀지는 것.

> 대시보드 API 는 `collector` 의 A 레코드를 **전부 조회해 합산**한다.
> 안 그러면 라운드로빈 때문에 매 틱마다 다른 인스턴스의 카운터가 잡혀 그래프가 널뛴다.
> 축소(1대)하면 종료된 인스턴스의 카운터가 사라져 누계가 줄어든 것처럼 보인다.
> Prometheus 카운터는 프로세스 수명 기준이라 그렇다. 운영에서는 Prometheus 가
> 인스턴스별 시계열을 따로 보관해 이런 착시가 없다.

---

### 8-2. `scenario_kafka_down.sh` — Kafka 정지 → fail-open → 재적재

부하가 흐르는 도중 브로커를 죽인다. **같은 장애에 두 컴포넌트가 다르게 반응하는 것**이 관전 포인트다.

**실측**

| 시점 | fallback(대기) | Outbox 미발행(지연) |
|---|---|---|
| 부하 중 | 0 (0) | 2 (0초) |
| 정지 +12초 | 1,088 (1,087) | 33 (5초) |
| 정지 +24초 | 4,036 (4,035) | 132 (21초) |
| 정지 +36초 | 6,941 (6,940) | 208 (32초) |
| 재기동 +15초 | 6,941 (6,940) | **8 (0초)** ← 스스로 배수 |
| 재적재 호출 후 | replayed **9,435**, failed 0 | 0 |

```
FAIL-OPEN kafka publish failed (streak=7500) topic=user.behavior ... -> writing to fallback file
FAIL-OPEN fallback file append: total=7500 file=/data/fallback/fallback-20260912-12.jsonl
```

파일은 `fallback-20260912-12.jsonl.done` 으로 rename 된다.

**대시보드에서 볼 것**

- `[수집]` fallback 과 **미재적재** 가 계단식으로 오름. 수신 카운터는 계속 올라감(=200 응답 유지).
- `[버퍼]` 모든 토픽 막대가 0 으로 죽음.
- `[정합성]` Outbox 미발행/최고 지연이 동시에 상승.
- 재기동하면 Outbox 는 **혼자 0 으로** 돌아가는데, Collector fallback 은 그대로 남아 있다.

> **Collector 는 가용성, ad-decision 은 정합성.** Collector 는 파일로 흘리고 200 을 주지만
> 복구에 사람 손이 필요하다(`POST /v1/admin/fallback/replay`). ad-decision 은 DB 에 남아
> 있어 워커가 알아서 따라잡지만 재발행 중복이 생긴다. 유실 vs 중복의 교환이 그대로 보인다.

---

### 8-3. `scenario_flink_kill.sh` — TaskManager kill → 체크포인트 복구

**실측**

```
kill 직전       ott-ads-realtime RUNNING    tasks 34/34  체크포인트 완료 40 실패 0
kill +6초       ott-ads-realtime RUNNING    tasks 34/34  ← 아직 모른다
하트비트 타임아웃 후  ott-ads-realtime RESTARTING tasks 0/34   체크포인트 실패 5
TM 재기동 39초 후  ott-ads-realtime RUNNING    tasks 34/34  체크포인트 완료 40

집계 연속성 검사 (kill 시각 이후)
  범위: 202609121248 ~ 202609121250 (총 3분)
  구멍 없음 — kill 이후 모든 분이 빠짐없이 집계됐다
```

**두 가지가 관전 포인트다.**

1. **죽여도 바로 안 죽는다.** JobManager 는 하트비트 타임아웃이 지나야 사망을 인지한다.
   그전까지 REST 는 `RUNNING tasks 34/34` 를 그대로 보고한다. 로컬은 15초로 줄여 놨고
   (`heartbeat.timeout: 15000`), **운영 기본값은 50초**다. "죽었는데 대시보드는 초록불"
   구간이 실제로 존재한다는 뜻이다.
2. **잡 상태만 보면 안 된다.** 슬롯이 없어도 잡은 `RUNNING` 으로 남고 `tasks` 만 0 이 된다.
   복구 판정은 `tasks.running == tasks.total` 로 해야 한다 (스크립트가 그렇게 기다린다).

집계에 구멍이 없는 이유는 10초 체크포인트에 **Kafka 오프셋까지 함께** 들어 있어
죽은 구간을 다시 읽기 때문이다. 대신 재처리 구간은 at-least-once 라 중복이 생기고,
그건 Spark 배치가 정리한다.

---

### 8-4. `scenario_burst.sh` — 지연 폭주 → late.events 급증

전송을 20초 멈췄다 재개한다. 워터마크 허용치(10초)보다 정지가 길어서
재개 순간 쏟아진 이벤트는 **이미 윈도우를 놓친 상태**로 도착한다.

**실측 — lateness 분포가 burst 의 서명이다**

```
총 12,098건
  0~ 10초 :    155 #######
 10~ 20초 :    646 ################################
 20~ 30초 :   3749 ##################################################
 30~ 40초 :   3714 ##################################################
```

`20~40초` 구간에 몰린 봉우리가 "정지 20초 + 생성기가 심은 30~60초 지연" 의 합성이다.

**대시보드에서 볼 것**

- `[처리]` late.events 가 **계단식**으로 뛴다 (정지 → 재개 주기마다 한 칸).
- `[버퍼]` 토픽 막대가 0 이었다가 폭증하는 톱니 모양.
- 생성기 콘솔의 `buf` 열이 0 → 수천 → 0 을 반복.

> **주의 — 대사 차이율은 크게 안 움직인다.** 6.47% → 6.52% 였다.
> 대사가 "하루 누적" 이라 burst 한 번은 희석되기 때문이다.
> burst 의 흔적은 차이율이 아니라 **lateness 분포와 late.events 증가량**에서 봐야 한다.
> 차이의 방향은 명확하다: 지연 이벤트는 실시간이 못 세고 배치는 세므로
> 대사 표의 `지연반영 +N` 항이 커진다 (+1,102 → +1,181).

---

### 8-5. `scenario_tamper.sh` — 서명 위조 급증 → DLQ + alert

VAST 트래킹 픽셀은 공개 URL 이라 누구나 호출할 수 있다.
HMAC 서명이 없으면 임프레션을 마음대로 부풀릴 수 있다는 뜻이다.

**실측**

```
평시    collector_dlq_total{reason="track_bad_signature"}  2,316
위조 후 collector_dlq_total{reason="track_bad_signature"} 21,517   (+19,201)
        생성기가 보고한 서명위조 건수                      19,201   ← 정확히 일치
```

집계에는 **한 건도 섞이지 않았다.** DLQ 원본에는 위조 쿼리스트링이 통째로 남는다.

```json
{"reason":"track_bad_signature","endpoint":"v1_track",
 "raw":{"eid":"evt-ac0f70be...","cid":"none","sig":"dc3193062147419c9fcccd11df25ac3c",
        "_query":"eid=...&sig=dc31930..."}}
```

부수 효과로 정상 임프레션이 사라져 **alert 가 5개 캠페인 전부에서 발화**했다.

```
13:02 cmp-1005  imp=2   req=17   ratio=0.118 (임계 0.5)
13:02 cmp-1003  imp=17  req=117  ratio=0.145 (임계 0.5)
13:02 cmp-1001  imp=15  req=69   ratio=0.217 (임계 0.5)
```

**대시보드에서 볼 것**

- `[수집]` DLQ 누적이 급등하고, 그 아래 사유 분포에서 `track_bad_signature` 만 자란다.
- `[정합성]` 캠페인별 imp/req 비율이 초록 → 빨강으로.
- `[경고]` alert.anomaly 목록이 채워진다.

> **여기서 배울 점.** 위조 픽셀에 4xx 를 주지 않고 **200 + 1x1 GIF** 로 답한다.
> 4xx 를 주면 플레이어가 재시도 폭주를 일으키기 때문이다. 방어는 HTTP 상태가 아니라
> "집계에 넣지 않는 것" 으로 한다. 서명 검증이 정산의 방어선이다.
>
> 그리고 이 상황의 alert 는 **"광고 사기" 가 아니라 "플레이어 장애" 로 오인되기 쉽다.**
> 둘 다 imp/req 비율이 무너지기 때문이다. 구분하려면 alert 를 DLQ 사유별 카운터와
> **같이** 봐야 한다. 대시보드에서 두 패널이 나란히 있는 이유가 이것이다.

---

### 8-6. 시나리오를 이어서 돌 때

시나리오는 서로의 상태를 남긴다 (fallback 파일, DLQ 카운터, Redis 집계).
깨끗하게 다시 보고 싶으면:

```bash
make clean && make up && make flink && make archive
```

각 시나리오 뒤에 확정 집계까지 보려면 항상 이 순서다.

```bash
make flush && make batch && make recon
```

---

## 9. 버전 관련 기록

임의로 버전을 올리지 않고, 충돌 지점을 남긴다.

1. **MinIO 이미지는 Docker Hub 에 없다.**
   `minio/minio`, `minio/mc` 저장소 자체가 Docker Hub 에서 제거되어 404 다.
   요청받은 `RELEASE.2024-09-13T20-26-02Z` 는 **quay.io 에 그대로 존재**하므로
   버전은 유지하고 레지스트리만 `quay.io/minio/...` 로 지정했다.

2. **Kafka 3.9 KRaft 컨트롤러 리스너.**
   `KAFKA_LISTENERS` 에 `CONTROLLER://0.0.0.0:9093` 을 쓰면 기동에 실패한다
   (`advertised.listeners cannot use the nonroutable meta-address 0.0.0.0`).
   3.9 는 컨트롤러의 광고 주소를 `listeners` 에서 파생시키기 때문이다.
   호스트를 비워 `CONTROLLER://:9093` 으로 두면 컨테이너 hostname 으로 파생된다.

3. **Flink 1.20 커넥터 (확인 완료).**
   Maven Central 에서 `-1.20` 접미사가 붙은 것만 골라 고정했다.
   - `flink-sql-connector-kafka:3.4.0-1.20`
   - `flink-connector-jdbc:3.3.0-1.20` — 3.3.0 부터 core 와 postgres dialect 가 한 jar 에 들어 있다
   - `postgresql:42.7.4` — JDBC 커넥터는 드라이버를 번들하지 않으므로 따로 넣는다

4. **Flink 1.20 용 Redis SQL 커넥터는 존재하지 않는다.** (§5-6 참조)
   버전을 올려 억지로 끼우지 않고 `Kafka(agg.minute) → redis-writer → Redis` 로 우회했다.

5. **Kafka 3.9 + Flink: 유휴 파티션.** 커넥터 문제는 아니지만 같은 급으로 자주 밟는다.
   `table.exec.source.idle-timeout` 을 켜지 않으면 트래픽이 희박한 토픽의 빈 파티션이
   전체 워터마크를 붙잡아 윈도우가 영원히 안 닫힌다. (§5-7)

6. **Spark 3.5.3 ↔ hadoop-aws (확인 완료).**
   `apache/spark:3.5.3` 이미지의 `hadoop-client-api` 가 **3.3.4** 다.
   - `hadoop-aws` 는 반드시 같은 **3.3.4**
   - hadoop-aws 3.3.4 는 AWS SDK **v1** 의 `aws-java-sdk-bundle:1.12.262` 로 빌드됐다
     (hadoop 3.4 부터 SDK v2 `bundle-2.x` 로 바뀐다. 섞으면 `NoClassDefFoundError`)
   - 280MB 라 저장소에 두지 않고 `spark/Dockerfile` 이미지 레이어로만 받는다

7. **Flink Parquet 은 Hadoop 을 따로 넣어야 한다.**
   `flink-sql-parquet` 은 parquet-hadoop 을 쓰지만 Hadoop 자체는 번들하지 않는다.
   넣지 않으면 INSERT 시점에 `ClassNotFoundException: org.apache.hadoop.conf.Configuration`.
   풀 배포판 대신 shaded 한 `hadoop-client-api` + `hadoop-client-runtime` 3.3.4 만 넣었다.
   `flink-s3-fs-hadoop` 은 `lib/` 가 아니라 **`plugins/s3-fs-hadoop/`** 에 넣어야 한다
   (lib 에 두면 Flink 셰이딩과 충돌. 공식 문서 지침).

8. **Flink SQL Client `-f` 는 `;` 뒤 같은 줄의 주석을 못 견딘다.** (§6-4)

---

## 10. 실제 운영과 다르게 단순화한 부분

이 목록이 "실제로 만들면 무엇이 더 필요한가" 의 체크리스트다.

| 항목 | 로컬 | 실제 운영 |
|---|---|---|
| Kafka 브로커 | 1대, RF=1, `acks=1` | 3~5대, RF=3, `acks=all` + `min.insync.replicas=2` |
| 파티션 | 6 | 48 |
| Flink 슬롯 | 2 (TM 1대) | 20 (TM 5대 × 4슬롯) |
| Kafka 데이터 디렉토리 | 컨테이너를 `root` 로 기동 | 전용 uid, 볼륨 사전 chown |
| MinIO | single drive | S3 (멀티 AZ) |
| Collector fallback | 컨테이너 로컬 디스크 | 사이드카 → S3 또는 로컬 mirror Kafka |
| HMAC 시크릿 | 단일 공유키, `.env` 평문 | 파트너별 키 + 롤링, KMS/Secret Manager |
| Admin 엔드포인트 | 수집 포트에 그대로 노출 | 별도 포트 + 인증 |
| Spark | 단일 컨테이너 on-demand | EMR / K8s executor 다수 |
| 생성기 시간 | 재생 위치를 x10~x150 배속 | 실시간 1배속, 사용자 수로 부하 조절 |
| Outbox 워커 | 단일 인스턴스, 잠금 없음 | 다중 인스턴스 + `FOR UPDATE SKIP LOCKED` |
| Outbox 중복 | `update-fail-rate` 로 인위 재현 | 실제 크래시/네트워크 단절로 발생 |
| 소재 결정 | 예산 가중 랜덤 | 타게팅 + 빈도제어 + 실시간 입찰 |
| Flink 병렬도 | 1 (슬롯 1개에 8태스크) | 20 (토픽별 분리 잡) |
| Flink 상태 백엔드 | hashmap(힙) + fs 체크포인트 | RocksDB + S3 체크포인트 |
| Flink→Redis | Kafka + redis-writer 우회 | 전용 싱크 커넥터 (또는 동일 구성) |
| 원본 적재 | Flink 파일 싱크 | Kafka Connect S3 Sink 또는 동일 구성 |
| Parquet 파일 크기 | 체크포인트(10초)마다 1개 → 수 KB | archive 잡 체크포인트 간격 ↑ + `auto-compaction`(128MB) 또는 Iceberg 컴팩션 |
| daily_settlement | 매번 전체 truncate 후 재적재 | 파티션 단위 MERGE / 증분 |
| 정산 금액 | CPM 만 | CPM + CPC + 완주 보너스 + 부가세/수수료 |
| 대사 주기 | 수동 실행 | 시간당 자동 + 임계 초과 시 알림 |
| 윈도우 마감 | `flush-windows.sh` 로 수동 유도 | 트래픽이 끊기지 않아 불필요 |
| Flink 체이닝 | 끔 (연산자별 계수 관찰용) | 켬 (처리량 우선) + 메트릭은 Prometheus reporter |
| 대시보드 인증 | 없음 | SSO + 읽기 권한 분리 |
| 대시보드 수집 | API 가 6곳을 직접 폴링 | Prometheus + Grafana (스크레이프 + 저장 + 알림) |
| Collector 스케일 | `docker compose --scale` 수동 | Kubernetes HPA (CPU/커스텀 메트릭 기반) |
| Flink 하트비트 | 15초 (관찰용으로 단축) | 50초 (기본값) |
| 장애 주입 | 스크립트로 컨테이너 stop/kill | Chaos Mesh / Gremlin 등 상시 카오스 테스트 |
| alert 전달 | Kafka 토픽 + 대시보드 목록 | PagerDuty / Slack + 에스컬레이션 정책 |
| 잡 배치 | 전 싱크를 한 잡에 | 도메인별 잡 분리 + 독립 스케일 |
| 생성기 규모 | 300~600 가상 사용자 | 수십만 동시 시청자 |
| 이상 케이스 | 생성기가 확률로 주입 | 실제로 발생 (재전송, 시계 오차, SSAI 구성) |

---

## 11. 산출물 구조

```
docker-compose.yml         전체 스택 (한 파일)
docker-compose.scale.yml   Collector 스케일 아웃 전용 오버레이 (시나리오 1)
.env                       로컬 축소 설정값 (파티션 수, 노필 비율, TTL 등)
Makefile / make.ps1        조작 진입점 (make 없는 Windows 용 래퍼 포함)
README.md                  이 문서

collector/                 Kotlin + Spring Boot WebFlux (수집 API, fail-open)
ad-decision/               Kotlin + Spring Boot MVC/JDBC (소재 결정 + Outbox 워커)
generator/                 Python asyncio (가상 시청자 + 이상 케이스 주입)
flink/                     Flink 이미지 (커넥터 JAR, Parquet, S3 플러그인)
sql/                       pipeline.sql (실시간) / archive.sql (원본 적재)
spark/                     Spark 이미지 + batch_settlement.py + reconcile.py
redis-writer/              agg.minute -> Redis 브리지 + 조회 API
dashboard/                 FastAPI + 단일 HTML/JS 관찰 화면 (8088)
tracer/                    FastAPI + 단일 HTML/JS 이벤트 추적기 (3000)
player/                    FastAPI + 단일 HTML/JS OTT 플레이어 화면 (3001)
kafka/                     토픽 생성 스크립트
postgres/init/             스키마 + 시드
scripts/                   운영/시나리오 스크립트
docs/                      주제별 문서 (흐름/저장소/광고API/규모/Kafka/학습)
data/                      공유 볼륨 (fallback, checkpoints, minio, 렌더된 SQL)
```

### 상시 기동 메모리 (실측)

| 컨테이너 | 사용 | 상한 |
|---|---|---|
| flink-tm | 744 MiB | 1.66 GiB |
| flink-jm | 633 MiB | 1.17 GiB |
| kafka | 615 MiB | 1.17 GiB |
| collector | 297 MiB | 640 MiB |
| minio | 234 MiB | 512 MiB |
| ad-decision | 228 MiB | 576 MiB |
| player | 78 MiB | 512 MiB |
| tracer / dashboard / redis-writer / postgres / redis | 205 MiB | 1.9 GiB |
| **합계** | **약 3.0 GiB** | 약 7.7 GiB |

(플레이어 추가 후 재측정. 잡 2개가 도는 상태 기준이라 이전 측정보다 Flink/Kafka 가 높다.)

Spark 는 `make batch` / `make recon` 때만 뜨고 최대 2 GiB 를 더 쓴다.
생성기도 `make load` 때만 뜬다 (최대 768 MiB). 8GB 예산 안에 들어간다.
