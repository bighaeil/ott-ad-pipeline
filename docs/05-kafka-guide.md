# 05. Kafka — 구조·개념·설계 판단 (이 저장소를 교재로)

Kafka 를 "메시지 큐" 로만 알고 있으면 이 파이프라인의 설계가 이해되지 않는다.
Kafka 는 **분산 커밋 로그**이고, 이 저장소의 거의 모든 판단이 그 성질에서 나온다.

읽는 순서: 2장(구조) → 5장(전달 보장) → 6장(이 프로젝트의 선택) → 9장(실습).
개념만 빠르게 보려면 10장의 대응표부터 봐도 된다.

---

## 1. 왜 여기에 Kafka 가 있나

플레이어가 만든 이벤트를 곧바로 집계 DB 에 쓰면 안 되는 이유는 셋이다.

| 문제 | Kafka 가 해결하는 방식 |
|---|---|
| **속도 차이** — 수집은 초당 수천 건, 집계/정산은 훨씬 느리다 | 디스크에 순차 기록하는 **버퍼**. 소비자가 느려도 생산자는 안 막힌다 |
| **재처리** — 집계 로직이 틀렸을 때 과거를 다시 계산해야 한다 | 메시지를 읽어도 **지우지 않는다**. 오프셋만 되감으면 다시 읽는다 |
| **여러 소비자** — 실시간 집계, 원본 적재, 알람, 추적기가 같은 데이터를 본다 | 컨슈머 그룹마다 **독립된 오프셋**. 한 번 쓰고 여럿이 읽는다(팬아웃) |

전통 MQ(RabbitMQ 등)는 소비하면 메시지가 사라진다. 그래서 위 3번이 어렵고 2번은 불가능하다.
이 프로젝트는 "원본을 남기고 나중에 다시 계산한다" 가 전제라 Kafka 가 아니면 성립하지 않는다.

> 대안 비교: **Kinesis/Pub-Sub** — 관리형이라 운영 부담이 적지만 보존/처리량 단가와 생태계가 다르다.
> **Pulsar** — 계층 분리(브로커 / BookKeeper)로 스토리지 확장이 유연. **RabbitMQ** — 작업 큐엔 낫지만
> 로그 재생이 목적이면 맞지 않는다.

---

## 2. 구조

```
Kafka 클러스터
├── 브로커 1, 2, 3 ...                (서버. 파티션의 리더/팔로워를 나눠 갖는다)
└── 토픽 "ad.impression"
    ├── 파티션 0 ─ [0][1][2][3][4] ...   ← 오프셋. append-only, 불변
    ├── 파티션 1 ─ [0][1][2] ...
    └── 파티션 5 ─ [0][1][2][3] ...
         └ 각 파티션은 세그먼트 파일들로 저장 (00000000000000000000.log + .index)
         └ RF=3 이면 리더 1 + 팔로워 2, ISR(In-Sync Replicas) 에 든 복제본만 승격 가능
```

핵심 성질 네 가지:

1. **순서는 파티션 안에서만 보장된다.** 토픽 전체 순서는 없다.
   → 그래서 "순서가 중요한 단위" 를 파티션 키로 잡아야 한다. 이 프로젝트는 `ad_request_id`.
2. **오프셋은 소비자가 관리한다.** 브로커는 누가 어디까지 읽었는지 신경 쓰지 않는다(`__consumer_offsets` 에 저장될 뿐).
3. **메시지는 보존 기간 동안 남는다.** 소비했다고 사라지지 않는다.
4. **파티션 수는 늘릴 수 있지만 줄일 수 없다.** 그리고 늘리면 키→파티션 매핑이 바뀌어
   **같은 키가 다른 파티션으로 간다**(순서 보장이 끊긴다). 그래서 처음에 넉넉히 잡는다.

### 빠른 이유 (면접 단골)

- **순차 I/O**: 랜덤 쓰기가 아니라 로그 끝에 append. HDD 에서도 빠르다
- **페이지 캐시**: JVM 힙이 아니라 OS 페이지 캐시를 쓴다 (그래서 Kafka 힙은 작게 잡는다)
- **zero-copy**: 디스크 → 소켓으로 커널에서 바로 전송(`sendfile`)
- **배치 + 압축**: 레코드를 묶어 압축해 전송·저장한다

### KRaft (ZooKeeper 없는 구성)

이 저장소도 KRaft 모드다. 메타데이터를 ZooKeeper 대신 Kafka 자체 Raft 쿼럼에 저장한다.
운영 요소가 하나 줄고, 컨트롤러 장애 복구가 빨라졌다.

---

## 3. 프로듀서가 정하는 것

| 설정 | 뜻 | 이 프로젝트 | 1억 규모 |
|---|---|---|---|
| **파티셔너** | key 해시 → 파티션. key 없으면 스티키 라운드로빈 | key = `ad_request_id` | 동일 |
| **`acks`** | 0=안 기다림 / 1=리더만 / all=ISR 전부 | 로컬 기본 | **all + `min.insync.replicas=2`** |
| **`enable.idempotence`** | 재시도 시 브로커에서 중복 제거(같은 세션 한정) | — | **true** |
| **`linger.ms` / `batch.size`** | 모아 보내기. 처리량↑ 지연↑ | 기본 | 20~50ms |
| **`compression.type`** | none/gzip/snappy/lz4/zstd | 없음 | lz4 또는 zstd |
| **`max.block.ms`** | 버퍼 가득/메타데이터 대기 시 블록 한계 | **2000** (fail-open 을 빨리 트리거) | 상황에 맞게 |

`COLLECTOR_KAFKA_MAX_BLOCK_MS=2000` 이 왜 중요한가: Kafka 가 죽었을 때
프로듀서가 기본값(60초)만큼 블록하면 Collector 스레드가 전부 잠기고 수집 자체가 멈춘다.
2초로 끊고 **로컬 파일로 흘려보낸 뒤 200 을 돌려주는 것**이 이 프로젝트의 fail-open 이다
([`EventPublisher.kt`](../collector/src/main/kotlin/com/ottads/collector/service/EventPublisher.kt)).

### 멱등 프로듀서는 무엇을 보장하지 않나

`enable.idempotence=true` 는 **같은 프로듀서 세션 안의 재시도 중복**만 막는다.
애플리케이션이 다시 시작해서 같은 이벤트를 또 보내면 새 메시지다.
이 프로젝트의 Outbox 재발행 중복이 정확히 그 경우라 **Kafka 기능으로는 못 막고**
downstream(`event_id` 중복제거)에서 처리한다.

---

## 4. 컨슈머가 정하는 것

- **컨슈머 그룹**: 같은 `group.id` 를 쓰는 인스턴스끼리 파티션을 나눠 갖는다.
  → **파티션 수 = 그룹 내 유효 병렬도 상한.** 파티션 6개에 컨슈머 10개를 붙이면 4개는 논다.
- **리밸런스**: 멤버가 들어오/나가면 파티션을 재배정한다. 그동안 소비가 멈춘다(stop-the-world).
  → 협력적 리밸런스(cooperative sticky)로 완화한다.
- **오프셋 커밋 시점이 전달 보장을 정한다.**

```
처리 → 커밋   : at-least-once  (처리 후 죽으면 재처리 = 중복)   ← 이 프로젝트
커밋 → 처리   : at-most-once   (커밋 후 죽으면 유실)
트랜잭션      : exactly-once   (Kafka→Kafka 한정, 비용 있음)
```

이 저장소의 컨슈머:

| 그룹 | 누구 | 토픽 | 커밋 |
|---|---|---|---|
| `flink-rt` | Flink 실시간 잡 | impression/quartile/click/request | 체크포인트에 맞춰 |
| `flink-archive` | Flink 원본적재 잡 | 위 + behavior | 체크포인트에 맞춰 |
| `redis-writer` | Redis 적재 | agg.minute, late.events, alert.anomaly | 자동 커밋 |
| `player-scan`, `tracer-scan` | 화면의 추적 기능 | 전 토픽 | **커밋 안 함**(assign 으로 직접 읽기) |

`player-scan` / `tracer-scan` 이 `subscribe` 가 아니라 `assign` 을 쓰는 이유:
그룹에 들어가면 리밸런스를 일으키고 오프셋을 건드린다. **관찰 도구가 파이프라인에 영향을 주면 안 된다.**

---

## 5. 전달 보장 — 세 가지를 구분하기

| 보장 | 뜻 | 대가 |
|---|---|---|
| at-most-once | 유실 가능, 중복 없음 | 데이터가 사라진다 |
| **at-least-once** | 유실 없음, **중복 가능** | 중복 제거를 직접 해야 한다 |
| exactly-once | 유실·중복 없음 | Kafka 내부(read-process-write)로 범위가 제한, 처리량 손해 |

**광고 정산에서 유실은 돈을 못 받는 것이고 중복은 과금 분쟁이다. 둘 다 나쁘지만 유실이 더 나쁘다** —
유실은 복구할 방법이 없고 중복은 나중에 걷어낼 수 있기 때문이다.
그래서 이 파이프라인은 전 구간 at-least-once 를 택하고 **중복 제거를 세 군데**에 둔다.

```
① Flink: event_id 중복제거 (상태 TTL 1시간)     → 재전송·Outbox 재발행 대부분
② Spark: event_id 전체범위 중복제거            → ①이 놓친 장기 중복
③ Spark: ad_request_id + source 우선순위       → SSAI 이중경로 (event_id 가 달라 ①②로 못 잡음)
```

"exactly-once 를 왜 안 쓰나" 에 대한 답: Flink 의 exactly-once 는 **Kafka→Kafka** 구간에서 성립한다.
이 파이프라인의 종착지는 Redis / S3 / PostgreSQL 이고, 그 싱크까지 원자적으로 묶으려면
2PC 또는 멱등 쓰기가 필요하다. 이 프로젝트는 **멱등 쓰기(배치 전체 재계산)** 로 같은 결과를 얻는다.

---

## 6. 이 프로젝트의 토픽·파티션 설계

### 토픽을 왜 이렇게 쪼갰나

```
ad.impression / ad.quartile / ad.click / ad.request / user.behavior / dlq.invalid
```

- **소비 패턴이 다르면 토픽을 나눈다.** `user.behavior` 는 전체의 63% 지만 실시간 집계에선 안 쓴다.
  한 토픽에 섞으면 실시간 잡이 쓸모없는 63% 를 계속 읽어 버린다.
- **보존/스키마가 다르면 나눈다.** `dlq.invalid` 는 구조가 아예 다르다(`raw` 통째로).
- 반대로 `ad_request` 와 `ad_response` 는 **같은 토픽**에 둔다. 같은 `ad_request_id` 키라
  같은 파티션에 모여야 둘을 짝지어 보기 쉽기 때문이다.

### 파티션 키를 `ad_request_id` 로 한 이유와 대가

| 선택지 | 장점 | 단점 |
|---|---|---|
| `campaign_id` | 캠페인별 집계 로컬리티 | **핫 파티션** — 대형 캠페인이 한 파티션을 독점 |
| `user_id` | 사용자 단위 분석 | 광고 1편의 이벤트가 흩어진다 |
| **`ad_request_id`** (선택) | 광고 1편의 request/impression/quartile/click 이 한 파티션에 순서대로 | 키 카디널리티가 매우 높아 캠페인 단위 집계는 셔플 필요 |

집계는 어차피 Flink 가 `campaign_id` 로 다시 키잉하므로 손해가 크지 않고,
**SSAI 이중경로 판정처럼 "광고 1편 단위" 로 봐야 하는 처리**가 로컬해지는 이득이 더 크다.

### 보존과 정리 정책

| 정책 | 언제 | 이 프로젝트 |
|---|---|---|
| `delete` (기본) | 시간/크기 기준 삭제 | 전 토픽. 로컬 1일 / 운영 7일 |
| `compact` | 키별 **최신 값만** 유지 | 안 씀. 쓴다면 "캠페인 메타 변경 스트림" 같은 상태성 토픽 |

보존 기간 = **재처리 가능 구간**이다. 7일이면 "일주일 안에 발견한 버그는 원본에서 다시 계산 가능" 이라는 뜻.
Parquet 원본이 따로 있으니 Kafka 보존을 무한정 늘릴 필요는 없다.

---

## 7. 운영에서 보는 지표

| 지표 | 정상 | 이상일 때 의미 |
|---|---|---|
| **Consumer lag** | 안정적으로 낮음 | 증가 = 소비자가 못 따라감 (파티션/병렬도/처리 병목) |
| **Under-replicated partitions** | 0 | >0 = 복제 지연/브로커 장애. 데이터 안전성 저하 |
| **ISR 축소** | 변화 없음 | 잦으면 네트워크/디스크 문제 |
| 브로커 디스크 사용률 | 보존 정책대로 안정 | 급증 = 보존 설정 오류 또는 트래픽 급증 |
| produce/fetch 지연 p99 | 수십 ms | 급등 = 디스크/페이지캐시 압박 |
| 파티션별 유입 편차 | 고름 | 한쪽 쏠림 = 핫 키 |

```bash
# 이 저장소에서 확인
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --all-groups     # lag
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --under-replicated-partitions
make topics                                                      # 파티션/리더 배치
```

---

## 8. 자주 만나는 장애와 원인

| 증상 | 흔한 원인 | 이 저장소에서 재현 |
|---|---|---|
| 프로듀서가 멈춘다 | 버퍼 가득 + `max.block.ms` 김 | `scripts/scenario_kafka_down.sh` |
| 중복 소비 | 리밸런스 중 커밋 실패, 재처리 | `OUTBOX_UPDATE_FAIL_RATE` 상향 |
| 컨슈머 랙 급증 | 파티션 < 병렬도, 처리 지연 | 부하 모드 `live` + Flink 병렬도 1 |
| 특정 파티션만 지연 | 핫 키 | 파티션 키를 `campaign_id` 로 바꿔 보면 바로 보인다 |
| 윈도우가 안 닫힘 | **유휴 파티션이 워터마크를 붙잡음** | 부하 정지 후 관찰 → `table.exec.source.idle-timeout` |

마지막 항목은 Kafka 자체 문제가 아니라 **Kafka 파티션 × 이벤트타임 처리**의 조합에서 나오는 함정이고,
실무에서 가장 자주 당한다.

---

## 9. 손으로 해 보는 실습 (예상 결과까지)

> 전제: `make up` 후 `bash scripts/flink-submit.sh` 까지 실행된 상태

**실습 1 — 메시지는 소비해도 사라지지 않는다**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ad.impression --from-beginning --max-messages 5
# 같은 명령을 두 번 실행 -> 같은 메시지가 또 나온다. (전통 MQ 라면 두 번째는 비어 있다)
```

**실습 2 — 파티션 키의 효과**
```bash
# 플레이어(:3001)에서 광고 1편을 본 뒤 그 ad_request_id 로 검색
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic ad.impression --from-beginning \
  --property print.partition=true --timeout-ms 5000 | grep req-play-
# 같은 ad_request_id 의 이벤트가 전부 같은 파티션 번호에 있다
```

**실습 3 — 컨슈머 그룹과 오프셋**
```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group flink-rt
# CURRENT-OFFSET / LOG-END-OFFSET / LAG 세 열의 관계를 본다
```

**실습 4 — 오프셋 되감기(재처리)**
```bash
bash scripts/flink-cancel.sh
STARTUP_MODE=earliest-offset bash scripts/flink-submit.sh
# 과거 이벤트를 처음부터 다시 처리한다. Redis 집계가 다시 채워진다.
# 이게 "재처리" 다. 원본을 안 지웠기 때문에 가능하다.
```

**실습 5 — fail-open (브로커 정지)**
```bash
bash scripts/scenario_kafka_down.sh
# Kafka 를 멈춰도 Collector 는 200 을 돌려주고 data/fallback 에 파일이 쌓인다.
# 복구 후 재적재하면 Kafka 에 중복이 생길 수 있다 -> 그래서 event_id 중복제거가 있다.
```

**실습 6 — 핫 파티션 만들기**
```bash
# 같은 ad_request_id 로 이벤트를 1000건 밀어 넣고 파티션별 분포를 본다
for i in $(seq 1 200); do
  curl -s -X POST localhost:8080/v1/events -H 'Content-Type: application/json' \
    -d "[{\"event_id\":\"evt-hot-$i\",\"event_type\":\"impression\",\"campaign_id\":\"cmp-1001\",
         \"event_time\":$(date +%s)000,\"ad_request_id\":\"req-HOTKEY\"}]" > /dev/null
done
# 전부 같은 파티션으로 들어간다. 운영에서 이러면 그 파티션의 컨슈머만 죽어난다.
```

**실습 7 — 지연과 워터마크**
```bash
bash scripts/scenario_burst.sh
# late.events 토픽이 채워지는 것을 대시보드 [처리] 패널에서 확인
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic late.events --max-messages 3 --from-beginning
```

---

## 10. 개념 → 이 저장소 대응표

| 개념 | 어디에 |
|---|---|
| 토픽 설계 | [`collector/.../model/Topics.kt`](../collector/src/main/kotlin/com/ottads/collector/model/Topics.kt), [`kafka/create-topics.sh`](../kafka/create-topics.sh) |
| 파티션 키 선택 | [`EventIngestService.ingest()`](../collector/src/main/kotlin/com/ottads/collector/service/EventIngestService.kt) `key = ad_request_id ?: event_id` |
| 프로듀서 설정 / fail-open | [`collector/.../config/KafkaConfig.kt`](../collector/src/main/kotlin/com/ottads/collector/config/KafkaConfig.kt), [`EventPublisher.kt`](../collector/src/main/kotlin/com/ottads/collector/service/EventPublisher.kt) |
| DLQ | [`EventIngestService.toDlq()`](../collector/src/main/kotlin/com/ottads/collector/service/EventIngestService.kt) |
| Outbox → Kafka | [`OutboxWorker.kt`](../ad-decision/src/main/kotlin/com/ottads/addecision/service/OutboxWorker.kt) |
| 컨슈머(스트림) | [`sql/pipeline.sql`](../sql/pipeline.sql), [`sql/archive.sql`](../sql/archive.sql) |
| 컨슈머(단순) | [`redis-writer/writer.py`](../redis-writer/writer.py) |
| 오프셋 직접 지정 | [`player/app.py`](../player/app.py) `scan_kafka()`, [`tracer/app.py`](../tracer/app.py) |
| 중복 제거 | `pipeline.sql` `deduped` 뷰, [`spark/batch_settlement.py`](../spark/batch_settlement.py) 2)3) |
| 워터마크/지각 | `pipeline.sql` `WATERMARK FOR event_time`, `late_events_sink` |

---

## 11. 학습 자료

### 먼저 읽을 것 (순서대로)

1. **Kafka 공식 문서 Design 장** — https://kafka.apache.org/documentation/#design
   로그 구조, 복제, 전달 보장이 원전 그대로. 20분이면 읽는다.
2. **Jay Kreps, "The Log"** — https://web.archive.org/web/20250105192530/https://engineering.linkedin.com/distributed-systems/log-what-every-software-engineer-should-know-about-real-time-datas-unifying
   (LinkedIn 원문 페이지는 삭제되어 404 다. 인터넷 아카이브 사본. 같은 글을 확장한 책이 『I ♥ Logs』(O'Reilly, 2014))
   왜 로그가 데이터 시스템의 중심인가. Kafka 의 설계 철학 자체.
3. **『데이터 중심 애플리케이션 설계』 (Martin Kleppmann)** 11장 스트림 처리
   Kafka 만이 아니라 "이벤트 로그" 라는 개념 전체를 정리해 준다. 이 프로젝트의 배경 지식 대부분이 여기 있다.

### 그다음

4. **『카프카 핵심 가이드』 2판** (Kafka: The Definitive Guide) — 프로듀서/컨슈머/운영 실무
5. **Confluent Developer** — https://developer.confluent.io/ (무료 코스, 개념 설명이 그림으로 잘 정리됨)
6. **Kafka 운영 문서** — https://kafka.apache.org/documentation/#operations (파티션 재배치, 보존, 모니터링)
7. **KRaft** — https://kafka.apache.org/documentation/#kraft

### 스트림 처리로 넘어갈 때

8. **Flink 공식 문서** — https://nightlies.apache.org/flink/flink-docs-stable/
   특히 *Concepts → Timely Stream Processing* (이벤트타임/워터마크)
9. **『Streaming Systems』 (Tyler Akidau)** — 워터마크·윈도우·트리거의 정본. 이 프로젝트의 지각 이벤트 처리가 이 책 내용이다.
10. **Flink Table/SQL** — https://nightlies.apache.org/flink/flink-docs-stable/docs/dev/table/overview/

### 패턴

11. **Outbox 패턴** — https://microservices.io/patterns/data/transactional-outbox.html
12. **Debezium(CDC)** — https://debezium.io/documentation/reference/stable/ (1억 규모에서 Outbox 폴링을 대체)

### 확인용 질문 (읽고 나서 답해 보기)

- 파티션을 나중에 늘리면 무엇이 깨지나?
- `acks=all` 인데도 데이터가 유실될 수 있는 경우는?
- 멱등 프로듀서가 막는 중복과 못 막는 중복은?
- 컨슈머 그룹에 컨슈머를 파티션 수보다 많이 붙이면?
- 오프셋 커밋을 처리 전에 하면 무엇이 달라지나?
- 이 프로젝트가 exactly-once 대신 at-least-once 를 택한 이유는?

답이 막히면 해당 개념의 장으로 돌아가고, 그다음 [06-learning-path.md](06-learning-path.md) 의 실습으로 확인한다.
