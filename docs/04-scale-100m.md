# 04. 1억 건 규모가 되면 무엇이 달라지는가

이 저장소는 8GB 노트북 한 대에서 도는 **축소판**이다.
여기서 관찰한 성질(중복·지각·SSAI·Outbox 중복)은 규모와 무관하게 똑같이 나타나지만,
**용량과 운영 구조는 전부 다시 계산해야 한다.** 이 문서는 그 계산과 변경점을 적는다.

숫자는 이 저장소의 실측값에서 출발한 **추정**이다. 실제 설계 시에는 자기 트래픽으로 다시 재야 한다.

---

## 1. 전제 — 무엇을 1억이라 부르는가

| 시나리오 | 뜻 | 평균 EPS | 피크 EPS(×5) |
|---|---|---|---|
| **S0 현재(로컬)** | 노트북 1대 | 200 ~ 2,000 (실측) | — |
| **S1 일 1억 건** | 중견 OTT 하루치 | 1,157 | 약 6,000 |
| **S2 시간당 1억 건** | 대형 라이브(결승전 등) | 27,778 | 약 8만 |
| **S3 일괄 재처리 1억 행** | 과거 1일치 다시 계산 | — | Spark 작업 |

실측 기준값 (이 저장소에서 잰 것):

| 항목 | 실측 | 출처 |
|---|---|---|
| Collector 1 인스턴스 처리량 | 약 2,000 EPS, p95 83ms | README 3-4 |
| 이벤트 1건 JSON 크기 | **443 B** | `ad.impression` 20건 평균 |
| 이벤트 1건 Parquet 크기 | 약 56 B (snappy) | 122,997행 / 6.9MB |
| 이벤트 구성비 | 광고 37% : 시청행동 63% | 생성기 실측 |
| 광고 요청(`ad_response`) 비율 | 전체의 약 4% | Parquet 집계 |

**S1 기준 파생 수치**: 광고 요청 약 400만 건/일(평균 46 TPS, 피크 230 TPS),
임프레션 약 450만 건/일.

---

## 2. 용량 산정 (S1 = 일 1억 건)

| 저장소 | 계산 | 하루 | 보존 적용 |
|---|---|---|---|
| Kafka (압축 전) | 1억 × 443B | 44 GB | — |
| Kafka (lz4 약 1/5, RF=3) | 44GB ÷ 5 × 3 | 27 GB | 7일 → **약 190 GB** |
| Parquet 원본 | 1억 × 56B | 5.6 GB | 1년 → **약 2 TB** |
| Redis 집계 | 캠페인 1,000 × 1,440분 × 250B | 0.36 GB | 48h → **약 0.7 GB** |
| PostgreSQL `event_outbox` | 400만 행 × 약 700B | 2.8 GB | 7일 정리 → **약 20 GB** |
| PostgreSQL 집계 | 캠페인 1,000 × 1,440분 | 144만 행/일 | 파티셔닝 필요 |

S2(시간당 1억)는 위 값을 **24배**로 읽으면 된다 — Kafka 하루 약 650 GB(압축·복제 후), Parquet 134 GB/일.
이 구간부터는 "Kafka 디스크가 아니라 네트워크와 컴팩션" 이 먼저 한계에 온다.

---

## 3. 컴포넌트별 변경

### 3-1. Collector (수집)

| 항목 | 현재 | S1 (일 1억) | S2 (시간당 1억) |
|---|---|---|---|
| 인스턴스 | 1 | **4~6** (피크 6K EPS ÷ 2K, 여유 2배) | **60~80** + 오토스케일 |
| 배치 크기 | `max-batch-size` 기본 | 500~1,000 | 1,000 |
| 프로듀서 | acks=1 수준 | `acks=all`, `linger.ms=20~50`, `compression=lz4` | 동일 + `batch.size` 상향 |
| fail-open 대상 | 로컬 파일 | **로컬 디스크 → 별도 재적재 잡** | 인스턴스가 죽으면 파일도 사라진다. 사이드카로 S3 업로드 |
| 앞단 | 없음 | L7 LB + keep-alive | LB + **Anycast/CDN 엣지 수집** |

**중요**: `linger.ms` 를 올리면 처리량이 오르고 지연이 는다.
이 파이프라인은 수집 지연이 수백 ms 늘어도 아무도 손해 보지 않는다(윈도우가 1분이다).
**처리량을 사는 게 맞는 거래다.**

### 3-2. Kafka

| 항목 | 현재 | S1 | S2 |
|---|---|---|---|
| 브로커 | 1 | 3 | 6~9 |
| 복제(RF) | 1 | 3 (`min.insync.replicas=2`) | 3 |
| 파티션 (`ad.impression`) | 6 | **48** | **192~256** |
| 보존 | 1일 | 7일 | 3일 + 티어드 스토리지(S3) |
| 압축 | 없음 | lz4 | lz4/zstd |

파티션 수 산정: `파티션 = max(목표 처리량 ÷ 파티션당 처리량, 컨슈머 병렬도)`.
파티션 하나가 안정적으로 소화하는 양을 5~10 MB/s 로 보면
S1 피크 6,000 EPS × 443B = 2.7 MB/s → 처리량만 보면 파티션 1개로도 된다.
**실제 결정 요인은 컨슈머 병렬도**다 — Flink 병렬도를 20으로 올리려면 파티션이 최소 20개 있어야 한다.
48은 "병렬도 20 + 향후 증설 여유 + 재배치 없이 늘릴 수 있게 2의 배수" 로 잡은 값이다.

**핫 파티션 주의**: 파티션 키가 `ad_request_id` 라 광고 1편의 이벤트는 7~8건이 한 파티션에 몰린다.
정상 트래픽에서는 고르게 퍼지지만, **버그로 `ad_request_id` 가 고정되면 한 파티션이 전부 받는다.**
S2에서는 파티션별 유입량 알람을 반드시 둔다.

### 3-3. Flink (실시간 처리)

| 항목 | 현재 | S1 | S2 |
|---|---|---|---|
| `parallelism.default` | 1 | **20** (TM 5대 × 4슬롯) | **100~200** |
| 상태 백엔드 | 힙 | **RocksDB + 증분 체크포인트** | 동일, 로컬 SSD 필수 |
| 체크포인트 간격 | 10초 | 30초~1분 | 1~2분 (정렬 없는 체크포인트) |
| 상태 TTL | 1시간 | 1시간 유지 (아래 계산 참조) | 30분으로 축소 검토 |
| 체크포인트 저장소 | 로컬 볼륨 | S3/HDFS | S3 + 리전 내 |
| 연산자 체이닝 | **false** (관찰용) | **true** (성능) | true |

**중복제거 상태 크기가 진짜 제약이다.** `event_id` 를 1시간 보관한다는 것은:

```
S1: 1억/일 ÷ 24 = 420만 건/시간 × (event_id 24B + 타임스탬프/오버헤드 ≈ 100B) ≈ 400 MB
S2: 1억/시간          × 약 100B                                          ≈ 10 GB
```

S1은 힙으로도 버틸 수 있지만, S2는 **RocksDB 가 아니면 OOM 이다.**
TTL 을 줄이면 상태는 작아지지만 **늦게 온 재전송을 실시간에서 못 잡는다** —
그만큼 배치가 메워야 할 몫(대사 차이)이 커진다. 이 교환을 문서에 명시하고 값을 정해야 한다.

`pipeline.operator-chaining` 을 `false` 로 둔 것은 이 저장소가 **연산자별 건수를 보여 주려고**
일부러 한 선택이다(대시보드 [처리] 패널). 운영에서는 직렬화 비용 때문에 반드시 `true` 로 돌린다.

### 3-4. 광고 결정과 Outbox

> **여기가 가장 먼저 깨진다.**

현재 구조는 **광고 요청 1건당 동기 DB INSERT 1건**이다.

| 항목 | 현재 | S1 (46~230 TPS) | S2 (1,100~5,500 TPS) |
|---|---|---|---|
| ad-decision 인스턴스 | 1 | 3~4 | 20~40 |
| DB 커넥션 | Hikari 15 | 인스턴스당 20~30 + **PgBouncer** | 동일 + 읽기 분리 |
| Outbox 워커 | 앱 내장 1개 (`SKIP LOCKED` 적용됨) | 워커 2~4 (`OUTBOX_WORKERS`) 또는 인스턴스 3~4 | 워커 8+, 또는 **CDC(Debezium)로 교체** |
| 폴링 주기/배치 | 500ms / 500행 | 200ms / 2,000행 | CDC 로 폴링 제거 |
| 테이블 정리 | 없음 | `published=true` **7일 후 삭제 + 파티셔닝** | 일 단위 파티션 DROP |

S1까지는 PostgreSQL 한 대로 충분하다. **S2에서는 Outbox 폴링 자체가 부담**이라
WAL 을 직접 읽는 CDC(Debezium → Kafka)로 바꾸는 것이 정석이다.
그러면 "폴링 지연 0.5초" 도 사라진다.

다중 워커 준비(`FOR UPDATE SKIP LOCKED` + 트랜잭션)는 이미 들어가 있다. 워커 3개로 돌려 중복 0건을
확인했다([03 문서](03-ad-decision-api.md) 참고). 남은 할 일은 하나다:

```kotlin
// OutboxWorker.kt
private val seen = ConcurrentHashMap.newKeySet<Long>()   // ← 200k 상한. 규모가 커지면 의미가 없다
```

`seen` 은 재발행 건수를 세기 위한 관찰용 집합이다. **운영 코드에서는 제거하거나 메트릭으로 대체**한다.

### 3-5. 저장 계층 — small files 와 컴팩션

지금 실측이 **파일 386개 / 평균 17.9 KB** 다. 원인은 Parquet(bulk 포맷)이 **체크포인트마다 파일을 닫기** 때문이다.
`rollover-interval` 을 1분으로 줘도 10초마다 닫힌다 ([08-minio-guide.md](08-minio-guide.md#4-flink-가-minio-에-쓰는-방식) 실측).
롤링 설정을 늘리는 것으로는 해결되지 않는다. 이대로 규모만 키우면:

```
파일 수/일 = 병렬도 × (86,400 ÷ 체크포인트 간격) × 토픽 수
S1: 20 × 8,640(10초) × 5 = 864,000 개/일   ← 메타데이터만으로 쿼리가 느려진다
    체크포인트를 1분으로 늘려도 144,000 개/일
```

대응(위에서부터 권장):

1. **archive 잡의 체크포인트 간격을 늘린다** (1~5분). 파일이 보이기까지의 지연과 맞바꾼다.
   실시간 잡과 잡이 분리돼 있으므로 집계 지연에는 영향이 없다.
2. **Flink 자동 컴팩션**: filesystem 커넥터에 `'auto-compaction' = 'true'`, `'compaction.file-size' = '128MB'`.
   체크포인트 사이에 생긴 작은 파일을 합친 뒤 커밋한다. 설정 두 줄로 가장 효과가 크다.
3. **별도 컴팩션 잡**: 시간 파티션이 닫힌 뒤 작은 파일을 합치는 배치를 따로 돌린다
3. **테이블 포맷 도입**: Iceberg / Delta.
   - 스냅샷 격리(읽는 중에 쓰기 가능), 스키마 진화, `rewrite_data_files` 컴팩션 내장,
     파티션 진화, 시간여행 복구를 공짜로 얻는다
   - Flink 는 Iceberg 싱크를 정식 지원한다 — 지금의 `filesystem` 커넥터를 교체하는 수준
4. **파티션 키 재검토**: `dt/hour` 에 더해 `source_topic` 을 넣으면 배치가 읽을 양이 줄어든다

### 3-6. Spark 배치

| 항목 | 현재 | S1 | S2 |
|---|---|---|---|
| 실행 | 수동 (`scripts/batch.sh`) | 시간 단위 스케줄 (Airflow 등) | 시간 단위 + 일 마감 |
| 범위 | **전체 재계산** | **증분** — 대상 `dt` 파티션만 | 증분 필수 |
| 기록 | `truncate` 후 전체 재적재 | 해당 `dt` 만 delete+insert (멱등 유지) | 파티션 교체 |
| 리소스 | 로컬 2GB | executor 10~20 × 4GB | 50~100 executor |
| 셔플 | 기본 | `spark.sql.shuffle.partitions` 를 데이터량에 맞춤 | AQE 활성 |

현재 코드의 `write_jdbc(mode="overwrite", truncate=True)` 는 **전체 테이블을 비우고 다시 쓴다.**
1억 행 규모에서는 이 한 줄 때문에 정산 테이블이 몇 분간 잠긴다.
`dt` 단위 upsert 또는 파티션 교체로 바꿔야 한다.

중복 제거 비용도 달라진다. `row_number() over (partition by event_id)` 는 **전 범위 셔플**이다.
1억 행이면 event_id 로 셔플하는 비용이 잡 전체를 지배한다.
→ 대상 파티션(dt)으로 범위를 좁히고, 그 경계를 넘는 중복은 "직전 일자까지 포함한 2일 윈도우" 로만 본다.

### 3-7. Redis / 서빙

| 항목 | 현재 | S1 | S2 |
|---|---|---|---|
| 구성 | 단일 인스턴스 | 단일 + 복제본(HA) | **클러스터 샤딩** |
| 키 설계 | `agg:1m:<cid>:<min>` | 그대로 유효 | 캠페인 수가 많으면 해시 태그로 슬롯 고정 |
| 조회 | `ZRANGEBYSCORE agg:index` | 그대로 | 인덱스도 분당으로 분할 |
| 금지 | — | `KEYS` 절대 금지 | 동일 |

### 3-8. 관측

| 항목 | 현재 | 규모가 커지면 |
|---|---|---|
| 대시보드 | 1초 폴링, 직접 Kafka/Redis/PG 조회 | Prometheus + Grafana. 대시보드가 원천을 직접 긁으면 안 된다 |
| 추적기/플레이어 | 오프셋부터 Kafka 전수 스캔 | 이 방식은 규모에서 못 쓴다. 샘플링 추적(trace_id 일부만) 또는 OpenTelemetry |
| 알람 | `alert.anomaly` 토픽 | 동일 + 컨슈머 랙, under-replicated partitions, 체크포인트 실패, Outbox 적체 |

---

## 4. 코드에서 실제로 바꿀 자리

| 파일 | 지금 | 1억 규모 |
|---|---|---|
| `.env` `KAFKA_PARTITIONS` | 6 | 48 (S1) / 192+ (S2) |
| `.env` `KAFKA_REPLICATION` | 1 | 3 |
| `.env` `KAFKA_RETENTION_MS` | 1일 | 7일 |
| `sql/pipeline.sql` `parallelism.default` | 1 | 20 ~ 200 |
| `sql/pipeline.sql` `pipeline.operator-chaining` | false | **true** |
| `sql/pipeline.sql` `table.exec.state.ttl` | 1h | 유지(+RocksDB) 또는 30m |
| `sql/pipeline.sql` `checkpointing.interval` | 10s | 30s~1m |
| `sql/archive.sql` `execution.checkpointing.interval` | 10s | 1~5 min (파일 크기 ↑, 보이기까지 지연 ↑) |
| `sql/archive.sql` `auto-compaction` | 없음 | `true` + `compaction.file-size = 128MB` |
| `sql/archive.sql` 커넥터 | `filesystem` | Iceberg |
| `OUTBOX_WORKERS` | 1 | 2~4 (조회에 `SKIP LOCKED` 는 이미 적용됨) |
| `OutboxWorker.kt` `seen` | 200k 집합 | 제거 (메트릭으로) |
| `application.yml` Hikari | max 15 | 20~30 + PgBouncer |
| `spark/batch_settlement.py` | 전체 재계산 + truncate | `dt` 증분 + 파티션 교체 |
| `spark/common.py` | 기본 설정 | AQE, shuffle partitions 튜닝 |
| `docker-compose.yml` collector | 1 replica | 4~6 (이미 `docker-compose.scale.yml` 로 실습 가능) |

---

## 5. 순서대로 깨진다 (부하를 올릴 때 나타나는 증상)

| 순서 | 증상 | 원인 | 대응 |
|---|---|---|---|
| 1 | `unpublished` 적체가 계속 증가 | Outbox 워커 1개의 발행 처리량 한계 | `OUTBOX_WORKERS` ↑ (SKIP LOCKED 적용됨), 배치 크기 ↑ |
| 2 | Collector p95 급등, fallback 파일 증가 | 인스턴스 부족 / Kafka 백프레셔 | Collector 수평 확장, linger·batch 조정 |
| 3 | Flink 체크포인트 실패, 백프레셔 | 상태가 힙을 넘음 | RocksDB + 증분 체크포인트, 병렬도 ↑ |
| 4 | 컨슈머 랙 증가 | 파티션 < 병렬도 | 파티션 증설(순서 보장 재검토 필요) |
| 5 | 배치 잡 시간이 하루를 넘김 | 전체 재계산 + small files | 증분 처리 + 컴팩션/Iceberg |
| 6 | 정산 테이블 잠금 | `truncate` 전체 재적재 | `dt` 단위 교체 |
| 7 | 대시보드가 원천을 무겁게 함 | 1초 폴링 직접 조회 | 메트릭 파이프라인 분리 |

**1번이 가장 먼저 온다.** 이 저장소에서도 `OUTBOX_UPDATE_FAIL_RATE` 를 올리면 같은 그림을 만들 수 있다.

---

## 6. 규모가 커져도 안 바뀌는 것

- **Outbox 패턴을 쓰는 이유** (DB 커밋과 Kafka 발행은 원자적일 수 없다)
- **at-least-once 를 택하고 중복은 뒤에서 걷어낸다**는 방침
- **실시간과 확정을 따로 계산하고 대사로 차이를 설명한다**는 구조 (람다 아키텍처)
- **원본을 가공 없이 보관한다**는 원칙 (집계 정의가 바뀌면 다시 계산해야 하니까)
- **파티션 키 = `ad_request_id`**, **중복제거 키 = `event_id`**, **SSAI 병합 키 = `ad_request_id`**

바뀌는 것은 **숫자와 운영 장치**일 뿐, 데이터 모델과 정합성 전략은 그대로다.
이 저장소의 축소판이 의미를 갖는 지점이 여기다.

---

## 7. 실제 검증 순서 (추정을 사실로 바꾸는 법)

1. `docker-compose.scale.yml` 로 Collector 를 늘리고 `scripts/scenario_live.sh` 로 피크 재현
2. 단일 인스턴스 한계 EPS 측정 → 필요한 인스턴스 수 = 목표 피크 ÷ 측정값 × 안전계수(2)
3. Flink 병렬도를 1 → 2 → 4 로 올리며 백프레셔가 사라지는 지점 확인
4. `OUTBOX_UPDATE_FAIL_RATE`, `FLINK_WATERMARK_DELAY` 를 흔들어 대사 차이가 어떻게 변하는지 기록
5. 위 표의 추정치를 측정치로 교체
