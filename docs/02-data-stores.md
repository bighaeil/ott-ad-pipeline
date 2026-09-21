# 02. 원본 데이터는 어디에 어떻게 쌓이고, 무엇에 쓰이는가

[01-event-flow.md](01-event-flow.md) 가 "이동 경로" 라면 이 문서는 **"착지 지점"** 이다.
저장소마다 스키마·보존기간·읽는 방법·쓰임을 적는다. 그대로 복사해 실행할 수 있는 조회 명령을 붙였다.

---

## 1. 저장소 지도

| 저장소 | 무엇이 | 가공 | 보존 | 누가 읽나 |
|---|---|---|---|---|
| **Kafka** | 모든 이벤트의 원본 JSON | 없음 (Collector 보강만) | 1일(로컬) / 7일(운영) | Flink 2개 잡, 추적기, 플레이어, 대시보드 |
| **MinIO (S3) Parquet** | 5개 토픽 전부, 영구 원본 | **없음** — 중복·지각·SSAI 그대로 | 무기한 (운영은 라이프사이클) | Spark 배치, 임의 분석 |
| **Redis** | 캠페인×분 단위 집계 | 중복제거 + 1분 윈도우 | TTL 48시간 | 대시보드, 대사 잡, 플레이어 |
| **PostgreSQL** | 캠페인 메타, Outbox, 확정 집계, 대사 결과 | 최종 가공 | 영구 | 정산, 대시보드, Flink lookup |
| **로컬 파일** `data/fallback` | Kafka 장애 때 못 보낸 이벤트 | 없음 | 재적재까지 | 운영자(수동 재적재) |

한 문장으로: **원본은 Parquet 에 다 남고, 빠른 답은 Redis 가, 최종 답은 PostgreSQL 이 갖는다.**

---

## 2. Kafka — 원본 JSON

### 2-1. 이벤트 레코드 (ad.impression 예시)

```json
{
  "event_id": "evt-play-329848582665d786",
  "event_type": "impression",
  "campaign_id": "cmp-1001",
  "ad_request_id": "req-play-7c504c2146da",
  "creative_id": "crt-1001-a",
  "session_id": "sess-play-1a2b3c",
  "user_id": "user-play-9f8e7d",
  "device": "smart_tv",
  "content_id": "ct-drama-201",
  "ad_pod_id": "pod-play-aa11bb",
  "ad_slot": 0,
  "ad_duration_s": 15,
  "playhead_s": 900,
  "source": "client",
  "transport": "batch",
  "event_time": "2026-09-18T07:05:17.412Z",   // Collector 가 ISO-8601 밀리초로 정규화
  "server_ts": "2026-09-18T07:05:17.455Z",    // Collector 수신 시각. 차이가 곧 지연
  "ingest_endpoint": "v1_events"              // v1_events | v1_track
}
```

Collector 가 붙이는 세 필드(`server_ts`, `event_time` 정규화, `ingest_endpoint`)만 보강이고,
나머지는 생산자가 보낸 그대로다. 모르는 필드도 지우지 않고 통과시킨다
(생성기의 `_late` / `_dup` 같은 표식이 뒤에서 되짚기용으로 살아남는다).

### 2-2. dlq.invalid — 버리지 않고 사유와 함께 남긴다

```json
{
  "dlq_id": "b2b0...",
  "reason": "missing_field:campaign_id",   // 또는 track_bad_signature, unknown_event_type:xxx
  "endpoint": "v1_events",
  "server_ts": "2026-09-18T07:05:17.4Z",
  "raw": { "...원본 그대로..." }
}
```

`reason` 별 건수가 곧 운영 지표다(대시보드 [수집] 패널의 "검증 실패" 분해).

### 2-3. 파생 토픽

| 토픽 | 스키마 요지 | 생산자 |
|---|---|---|
| `agg.minute` | window_start/end, campaign_id, advertiser, campaign_name, impressions, clicks, completes, requests, imp_req_ratio, ssai_dupes, emitted_at | Flink |
| `late.events` | event_id, event_type, campaign_id, ad_request_id, event_time, window_end, watermark_at, **lateness_ms**, topic_origin — 속한 창이 닫힌 뒤 도착한 것만 | Flink |
| `alert.anomaly` | alert_type, campaign_id, window, impressions, requests, imp_req_ratio, threshold, message, detected_at | Flink |

### 2-4. 읽는 법

```bash
# 토픽 목록과 파티션/오프셋
make topics

# 원문 3건
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ad.impression --from-beginning --max-messages 3

# 특정 광고만 보기
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ad.request --from-beginning --timeout-ms 5000 \
  | grep 'req-play-'

# 컨슈머 랩 (누가 얼마나 밀렸나)
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups
```

---

## 3. Redis — 실시간 서빙 계층

[`redis-writer/writer.py`](../redis-writer/writer.py) 가 만드는 키는 다섯 종류다.

| 키 | 타입 | 내용 | TTL |
|---|---|---|---|
| `agg:1m:<campaign_id>:<yyyyMMddHHmm>` | HASH | 그 분의 집계 | 48h |
| `agg:campaigns` | SET | 등장한 campaign_id | 48h |
| `agg:index` | ZSET | score=window_start epoch, member=위 키 | (구간 정리) |
| `late:1m:<yyyyMMddHHmm>`, `late:total` | STRING | 지각 이벤트 카운터 | 48h |
| `alerts:recent`(최근 100건), `alerts:total` | LIST/STRING | 이상 감지 경고 | 48h |

HASH 필드 (실측):

```
$ docker compose exec redis redis-cli hgetall agg:1m:cmp-1001:202609180707
campaign_id     cmp-1001
advertiser      한빛통신
campaign_name   5G 무제한 요금제
impressions     1
clicks          0
completes       0
requests        1
imp_req_ratio   1.0
ssai_dupes      0
window_start    2026-09-18T07:07:00Z
window_end      2026-09-18T07:08:00Z
updated_at      2026-09-18T07:08:31.857+00:00
```

`agg:index` 를 따로 두는 이유: `KEYS agg:1m:*` 는 O(N) 풀스캔이라 운영에서 쓰면 안 된다.
구간 조회를 ZSET 의 `ZRANGEBYSCORE` 로 하면 O(log N + M) 이다.

```bash
# 구간 합계 / 시계열 (대시보드와 대사 잡이 쓰는 바로 그 API)
curl -s 'http://localhost:8099/agg/summary?dt=2026-09-18' | python -m json.tool
curl -s 'http://localhost:8099/agg/series?minutes=30' | head -c 600
```

**왜 Redis 인가**: 대시보드가 1초마다 폴링하는데 매번 Parquet 을 스캔할 수 없고,
PostgreSQL 에 초당 수백 번 쓰면 정산 테이블과 경합한다. 실시간 값은 어차피 48시간 뒤면
쓸모가 없어서 휘발성 저장소가 맞다.

---

## 4. MinIO Parquet 원본

### 4-1. 경로와 파티셔닝

```
s3a://events/dt=2026-09-18/hour=07/part-<jobid>-<subtask>-<n>
                │            │
                │            └ 시(UTC). event_time 기준
                └ 날짜(UTC). event_time 기준 — 서버 도착 시각이 아니다
```

Hive 스타일 파티션이라 Spark / pyarrow / DuckDB 가 그대로 인식하고,
`dt=` 조건을 걸면 그 폴더만 읽는다(파티션 프루닝).

### 4-2. 스키마 (26 컬럼)

| 그룹 | 컬럼 |
|---|---|
| 식별 | `event_id`, `event_type`, `campaign_id`, `ad_request_id`, `creative_id` |
| 사용자/콘텐츠 | `session_id`, `user_id`, `device`, `content_id` |
| 광고 | `quartile`, `ad_pod_id`, `ad_slot`, `ad_duration_s`, `playhead_s`, `fill` |
| 경로 | `source`(client/server), `transport`, `ssai_twin_of`, `ingest_endpoint`, `server_ts` |
| 표식 | `late_flag`, `dup_flag` (생성기가 심은 이상 케이스 표시) |
| 시간/출처 | `event_time`, `source_topic`, `dt`, `hour` |

`ssai_twin_of` 와 `late_flag`/`dup_flag` 가 있어서 **"이 차이가 왜 났는지"** 를
배치에서 되짚을 수 있다. 원본을 가공 없이 남기는 이유가 이것이다.

### 4-3. 실측 규모 (이 로컬 환경 기준)

| 항목 | 값 |
|---|---|
| 적재 행수 | 122,997행 |
| 파일 수 | 386개 |
| 총 크기 | 6.9 MB (snappy) |
| 행당 평균 | 약 56 B (같은 이벤트의 Kafka JSON 원문은 **443 B** — 실측) |
| **파일당 평균** | **약 17.9 KB** ← 작은 파일 문제 |

Parquet 이 JSON 보다 약 8배 작다(컬럼 단위 인코딩 + 반복 값 압축).
반면 파일 하나가 18KB밖에 안 되는 것은 **전형적인 small files 문제**다.
로컬에서는 문제가 안 되지만 1억 건 규모에서는 이것부터 깨진다
→ [04-scale-100m.md](04-scale-100m.md#3-5-저장-계층--small-files-와-컴팩션).

### 4-4. 직접 까 보기

```bash
# (a) 이미 pyarrow 가 들어 있는 컨테이너에서 바로
docker compose exec player python -c "
import pyarrow.dataset as pads
from pyarrow import fs as pafs
s3 = pafs.S3FileSystem(endpoint_override='minio:9000', access_key='minioadmin',
                       secret_key='minioadmin', scheme='http', region='us-east-1')
ds = pads.dataset('events', filesystem=s3, format='parquet', partitioning='hive')
t = ds.to_table(columns=['event_id','event_type','campaign_id','source','dt'])
import collections; print(t.num_rows, collections.Counter(t.column('event_type').to_pylist()).most_common())
"

# (b) 특정 광고 1편만
docker compose exec player python -c "
import pyarrow.dataset as pads
from pyarrow import fs as pafs
s3 = pafs.S3FileSystem(endpoint_override='minio:9000', access_key='minioadmin',
                       secret_key='minioadmin', scheme='http', region='us-east-1')
ds = pads.dataset('events', filesystem=s3, format='parquet', partitioning='hive')
print(ds.to_table(filter=pads.field('ad_request_id')=='req-play-XXXX').to_pydict())
"

# (c) MinIO 콘솔에서 눈으로  ->  http://localhost:9001  (minioadmin/minioadmin)
```

---

## 5. PostgreSQL

[`postgres/init/01-schema.sql`](../postgres/init/01-schema.sql) + [`02-late-dropped.sql`](../postgres/init/02-late-dropped.sql) + 배치가 런타임에 만드는 테이블 1개.

| 테이블 | 쓰는 쪽 | 읽는 쪽 | 성격 |
|---|---|---|---|
| `campaigns` | 운영자(시드) | ad-decision 캐시, Flink lookup join, 플레이어 화면 | 차원 테이블 |
| `campaign_rates` | 운영자 | ad-decision(CPM 응답), Spark 정산 | 단가 |
| `event_outbox` | ad-decision (결정과 **같은 트랜잭션**) | Outbox 워커 | 발행 대기열 |
| `daily_settlement` | Spark 배치 (truncate 후 전체 재적재 = 멱등) | 대시보드, 정산 | 확정 집계(일) |
| `minute_settlement` | Spark 배치 | 대시보드 그래프(확정 선), 구간 대사 | 확정 집계(분). `raw_impressions` 포함 |
| `late_dropped` | Flink (`event_id` upsert) | 대사 | 실시간 윈도우가 버린 이벤트 |
| `reconciliation` | Spark 대사 | 대시보드 "원인 분해" | 실시간 vs 확정 차이 (`scope` = 대사 범위) |

### 5-1. event_outbox 가 이 설계의 핵심이다

```sql
CREATE TABLE event_outbox (
  id           BIGSERIAL PRIMARY KEY,
  event_id     TEXT NOT NULL,
  event_type   TEXT NOT NULL,        -- 'ad_response'
  aggregate_id TEXT NOT NULL,        -- ad_request_id (= Kafka 파티션 키)
  payload      JSONB NOT NULL,       -- Kafka 로 그대로 나갈 본문
  occurred_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  published    BOOLEAN NOT NULL DEFAULT FALSE,
  published_at TIMESTAMPTZ
);
CREATE INDEX idx_outbox_unpublished ON event_outbox (id) WHERE published = FALSE;  -- 부분 인덱스
```

`event_id` 에 UNIQUE 를 **일부러 걸지 않았다**. 워커가 발행 후 플래그 업데이트에 실패하면
같은 행이 재발행되어 Kafka 에 중복이 생기는데, 그게 Outbox 패턴의 정의된 성질(at-least-once)이고
이 프로젝트가 관찰하려는 대상이기 때문이다. 재현 확률은 `OUTBOX_UPDATE_FAIL_RATE`(기본 0.02).

```sql
-- 적체 확인
SELECT count(*) FILTER (WHERE NOT published) AS unpublished,
       max(now() - occurred_at) FILTER (WHERE NOT published) AS oldest
FROM event_outbox;

-- 한 광고의 서버 확정 이벤트
SELECT id, event_id, published, occurred_at, published_at, payload->>'campaign_id'
FROM event_outbox WHERE aggregate_id = 'req-play-XXXX';
```

### 5-2. daily_settlement — 정산의 최종 답

| 컬럼 | 뜻 |
|---|---|
| `requests` | 서버가 확정해 **실제로 채운** 광고 수 (`ad_response AND fill=true`) |
| `impressions` | event_id 중복 제거 + SSAI 정리까지 끝낸 확정값 → **과금 기준** |
| `raw_impressions` | event_id 중복만 제거하고 SSAI 는 안 뺀 값 → 실시간(Flink)과 같은 지점 |
| `dupes_removed` | event_id 중복으로 접힌 건수 |
| `late_impressions` | 생성기가 `_late`(지연 전송) 표식을 붙인 건수. **참고용** — 실시간이 실제로 버린 건수는 `late_dropped` |
| `clicks`, `completes` | 클릭 / 완주 |
| `cpm`, `amount` | `amount = impressions / 1000 * cpm` |

`raw_impressions - impressions` 가 곧 **SSAI 이중경로로 부풀었던 양**이다.

```sql
SELECT dt, campaign_id, requests, impressions, raw_impressions,
       raw_impressions - impressions AS ssai_removed,
       dupes_removed, late_impressions, amount
FROM daily_settlement ORDER BY dt DESC, campaign_id;
```

### 5-3. reconciliation — 차이의 장부

```sql
SELECT run_at, scope, campaign_id, realtime_impressions, batch_impressions,
       diff, diff_rate, likely_cause
FROM reconciliation ORDER BY run_at DESC LIMIT 10;
```

`likely_cause` 는 `확정 − 실시간 = 지연반영(late_dropped) − SSAI + 잔차` 로 분해한 결과다. 잔차는 0 이 정상이다.

### 5-4. late_dropped — 실시간이 버린 것의 명단

Flink 실시간 잡이 "속한 1분 창이 이미 닫혀 윈도우가 버린 이벤트" 를 upsert 한다
([`postgres/init/02-late-dropped.sql`](../postgres/init/02-late-dropped.sql), 기존 볼륨에는 `flink-submit.sh` 가 적용).

| 컬럼 | 뜻 |
|---|---|
| `event_id` | 기본키. Flink 가 재처리로 같은 이벤트를 다시 써도 한 행 |
| `kind` | impression / quartile / complete / click / request |
| `event_time`, `window_end`, `watermark_at` | UTC. `watermark_at >= window_end - 1ms` 인 것만 들어온다 |

Kafka `late.events` 에도 같은 이벤트가 가지만, 그쪽을 세는 Redis `late:total` 은 재처리하면 두 번 센다.
**대사는 이 테이블로 센다.**

```sql
-- 구간 안에서 실시간이 버린 impression
SELECT campaign_id, count(*) FROM late_dropped
WHERE kind = 'impression' AND event_time >= '2026-09-21 08:31' AND event_time < '2026-09-21 08:34'
GROUP BY 1 ORDER BY 1;
```

---

## 6. 원본은 무엇에 쓰이나 (활용 3가지)

### 6-1. 정산 — 돈이 되는 값 만들기

`Parquet → (중복제거 → SSAI 정리 → 집계 → CPM 조인) → daily_settlement`.
실시간 값으로 청구하지 않는 이유는 위 표대로 실시간이 **부풀거나 모자라기** 때문이다.

### 6-2. 대사 — 두 숫자가 다른 이유를 설명하기

광고주가 "왜 리포트 숫자가 대시보드와 다르냐" 고 물을 때 답할 수 있어야 한다.
`reconciliation` 이 그 답을 미리 계산해 둔 장부다.

### 6-3. 재처리 / 새 지표 — 원본을 남긴 진짜 이유

집계 정의가 바뀌거나 버그를 고쳤을 때, **원본이 있으면 과거를 다시 계산할 수 있다.**
Redis 만 있었으면 지난 값은 영영 틀린 채로 남는다. 예:

```python
# "광고를 절반 이상 본 사람 수" 라는 지표를 나중에 추가한다면 — 원본만 다시 읽으면 된다
mid = ds.to_table(filter=(pads.field('event_type')=='quartile') &
                         (pads.field('quartile')=='midpoint'))
```

```sql
-- 디바이스별 완주율 (원본 그대로)  * Spark 컨테이너에서
SELECT device,
       count(*) FILTER (WHERE quartile='complete')::float
       / NULLIF(count(*) FILTER (WHERE event_type='impression'),0) AS completion_rate
FROM parquet_events GROUP BY device;
```

---

## 7. 보존·정리 정책

| 저장소 | 로컬 | 운영 권장 | 근거 |
|---|---|---|---|
| Kafka | 1일 | 7일 | 장애 복구 시 재처리 가능 구간 = 보존기간 |
| Redis | 48h | 48h | 실시간 값의 유효기간 |
| Parquet | 무기한 | 핫 90일(표준) → 이후 아카이브 계층 | 재처리/감사 |
| PostgreSQL 집계 | 무기한 | 무기한 | 정산 근거 |
| `event_outbox` | 무기한 | **published=true 는 7일 후 삭제** | 안 지우면 테이블이 무한 증가 |
| `data/fallback` | 재적재까지 | 재적재 후 삭제 | 이중 집계 방지 |

운영에서 추가로 필요한 것: `user_id` 는 개인정보다. 원본 보존 기간을 정하고,
분석용 테이블에는 해시/치환한 ID 만 남기고, 삭제 요청(GDPR 등) 처리 경로를 둬야 한다.
이 로컬 구현에는 그 장치가 없다 — 축소한 부분이다.
