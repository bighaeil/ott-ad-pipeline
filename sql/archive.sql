-- ===========================================================================
-- 원본 적재 (Flink 파일 싱크 -> MinIO Parquet)
--
-- 실행:  bash scripts/flink-archive.sh   (또는 make archive)
--
-- [Flink SQL Client 주의] -f 모드는 ';' 로 문장을 자른 뒤 각 조각을 파싱한다.
-- 그래서 ';' 뒤에 같은 줄로 주석을 달면 그 주석이 다음 조각의 앞에 붙어
--   java.lang.IllegalArgumentException: only single statement supported
-- 로 죽는다. 주석은 반드시 문장 "위" 줄에 쓸 것.
--
-- 경로:  s3a://events/dt=YYYY-MM-DD/hour=HH/part-*.parquet
-- 가공하지 않는다. 필터도, 중복 제거도, 집계도 없다.
-- 지연·중복·SSAI 이중경로가 전부 그대로 들어간다. 그걸 걷어내는 건 Spark 배치 몫이다.
--
-- Kafka Connect 대신 Flink 파일 싱크를 고른 이유
--   1) Confluent S3 Sink 는 Confluent Community License 라 배포 조건이 다르다.
--   2) Connect 워커 컨테이너가 하나 더 필요하다 (로컬 8GB 예산에 부담).
--   3) Flink 는 이미 떠 있고, 체크포인트에 맞춰 파일을 커밋하므로
--      exactly-once 파일 커밋이 공짜로 따라온다.
--
-- 슬롯 예산: 이 잡이 슬롯 1개를 쓴다. pipeline.sql 이 1개 -> 로컬 2슬롯을 꽉 채운다.
--            운영(20슬롯)에서는 토픽별로 잡을 나누고 병렬도를 올린다.
-- ===========================================================================

SET 'pipeline.name' = 'ott-ads-archive';
-- 로컬 1 / 운영 4~8
SET 'parallelism.default' = '1';
-- 체크포인트 주기 = 파일 커밋 주기다. 이 값이 곧 "Parquet 로 보이기까지의 지연".
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';
SET 'table.local-time-zone' = 'UTC';
SET 'table.exec.source.idle-timeout' = '__IDLE_TIMEOUT__ s';

-- ===========================================================================
-- 소스. pipeline.sql 과 컬럼이 다르다 (여기는 원본 보존이 목적이라 필드를 다 받는다).
-- SQL Client 세션은 잡마다 따로라 DDL 을 다시 선언해야 한다.
-- ===========================================================================
CREATE TABLE src_impression (
    event_id STRING, event_type STRING, campaign_id STRING, ad_request_id STRING,
    creative_id STRING, session_id STRING, user_id STRING, device STRING,
    content_id STRING, quartile STRING, ad_pod_id STRING, ad_slot INT,
    ad_duration_s INT, playhead_s DOUBLE, `source` STRING, transport STRING,
    ssai_twin_of STRING, ingest_endpoint STRING, server_ts STRING,
    `_late` BOOLEAN, `_dup` BOOLEAN,
    event_time TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka', 'topic' = 'ad.impression',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-archive',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE src_quartile (
    event_id STRING, event_type STRING, campaign_id STRING, ad_request_id STRING,
    creative_id STRING, session_id STRING, user_id STRING, device STRING,
    content_id STRING, quartile STRING, ad_pod_id STRING, ad_slot INT,
    ad_duration_s INT, playhead_s DOUBLE, `source` STRING, transport STRING,
    ssai_twin_of STRING, ingest_endpoint STRING, server_ts STRING,
    `_late` BOOLEAN, `_dup` BOOLEAN,
    event_time TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka', 'topic' = 'ad.quartile',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-archive',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE src_click (
    event_id STRING, event_type STRING, campaign_id STRING, ad_request_id STRING,
    creative_id STRING, session_id STRING, user_id STRING, device STRING,
    content_id STRING, quartile STRING, ad_pod_id STRING, ad_slot INT,
    ad_duration_s INT, playhead_s DOUBLE, `source` STRING, transport STRING,
    ssai_twin_of STRING, ingest_endpoint STRING, server_ts STRING,
    `_late` BOOLEAN, `_dup` BOOLEAN,
    event_time TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka', 'topic' = 'ad.click',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-archive',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE src_request (
    event_id STRING, event_type STRING, campaign_id STRING, ad_request_id STRING,
    creative_id STRING, session_id STRING, user_id STRING, device STRING,
    content_id STRING, quartile STRING, ad_pod_id STRING, ad_slot INT,
    ad_duration_s INT, playhead_s DOUBLE, `source` STRING, transport STRING,
    ssai_twin_of STRING, ingest_endpoint STRING, server_ts STRING,
    fill BOOLEAN, `_late` BOOLEAN, `_dup` BOOLEAN,
    event_time TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka', 'topic' = 'ad.request',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-archive',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE src_behavior (
    event_id STRING, event_type STRING, campaign_id STRING, ad_request_id STRING,
    creative_id STRING, session_id STRING, user_id STRING, device STRING,
    content_id STRING, quartile STRING, ad_pod_id STRING, ad_slot INT,
    ad_duration_s INT, playhead_s DOUBLE, `source` STRING, transport STRING,
    ssai_twin_of STRING, ingest_endpoint STRING, server_ts STRING,
    `_late` BOOLEAN, `_dup` BOOLEAN,
    event_time TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka', 'topic' = 'user.behavior',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-archive',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json', 'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

-- ===========================================================================
-- 싱크. dt/hour 로 파티션한다.
--
-- 파일 커밋 시점 = 체크포인트 완료 시점. 그래서 체크포인트가 실패하면 파일이 안 보인다.
-- Parquet 은 bulk 포맷이라 아래 롤링 정책(시간/크기)과 상관없이 체크포인트마다 파일을 닫는다.
-- 실제 롤링 = min(rollover-interval, 체크포인트 간격 10초). 그래서 파일은 10초 안에 보이지만
-- 잘게 쪼개진다 (실측: writer 마다 정확히 10초 간격으로 part 파일 생성, docs/08-minio-guide.md 4절).
-- ===========================================================================
CREATE TABLE events_archive (
    event_id STRING, event_type STRING, campaign_id STRING, ad_request_id STRING,
    creative_id STRING, session_id STRING, user_id STRING, device STRING,
    content_id STRING, quartile STRING, ad_pod_id STRING, ad_slot INT,
    ad_duration_s INT, playhead_s DOUBLE, `source` STRING, transport STRING,
    ssai_twin_of STRING, ingest_endpoint STRING, server_ts STRING,
    fill BOOLEAN, late_flag BOOLEAN, dup_flag BOOLEAN,
    event_time TIMESTAMP(3),
    source_topic STRING,
    dt STRING,
    `hour` STRING
) PARTITIONED BY (dt, `hour`) WITH (
    'connector' = 'filesystem',
    'path' = 's3a://events/',
    'format' = 'parquet',
    'sink.rolling-policy.rollover-interval' = '__ROLLOVER__',
    'sink.rolling-policy.check-interval' = '10 s',
    'sink.rolling-policy.file-size' = '16MB',
    'sink.partition-commit.trigger' = 'process-time',
    'sink.partition-commit.delay' = '0 s',
    'sink.partition-commit.policy.kind' = 'success-file',
    'parquet.compression' = 'snappy'
);

-- ===========================================================================
-- 다섯 토픽을 그대로 흘려 넣는다.
-- TIMESTAMP_LTZ -> TIMESTAMP 로 캐스팅하는 이유: Parquet 로 쓸 때 로컬 타임존
-- 해석이 개입하지 않게 UTC 벽시계 값으로 고정한다 (table.local-time-zone=UTC).
-- ===========================================================================
EXECUTE STATEMENT SET
BEGIN

INSERT INTO events_archive
SELECT event_id, event_type, campaign_id, ad_request_id, creative_id, session_id,
       user_id, device, content_id, quartile, ad_pod_id, ad_slot, ad_duration_s,
       playhead_s, `source`, transport, ssai_twin_of, ingest_endpoint, server_ts,
       CAST(NULL AS BOOLEAN), `_late`, `_dup`,
       CAST(event_time AS TIMESTAMP(3)),
       'ad.impression',
       DATE_FORMAT(event_time, 'yyyy-MM-dd'), DATE_FORMAT(event_time, 'HH')
FROM src_impression;

INSERT INTO events_archive
SELECT event_id, event_type, campaign_id, ad_request_id, creative_id, session_id,
       user_id, device, content_id, quartile, ad_pod_id, ad_slot, ad_duration_s,
       playhead_s, `source`, transport, ssai_twin_of, ingest_endpoint, server_ts,
       CAST(NULL AS BOOLEAN), `_late`, `_dup`,
       CAST(event_time AS TIMESTAMP(3)),
       'ad.quartile',
       DATE_FORMAT(event_time, 'yyyy-MM-dd'), DATE_FORMAT(event_time, 'HH')
FROM src_quartile;

INSERT INTO events_archive
SELECT event_id, event_type, campaign_id, ad_request_id, creative_id, session_id,
       user_id, device, content_id, quartile, ad_pod_id, ad_slot, ad_duration_s,
       playhead_s, `source`, transport, ssai_twin_of, ingest_endpoint, server_ts,
       CAST(NULL AS BOOLEAN), `_late`, `_dup`,
       CAST(event_time AS TIMESTAMP(3)),
       'ad.click',
       DATE_FORMAT(event_time, 'yyyy-MM-dd'), DATE_FORMAT(event_time, 'HH')
FROM src_click;

INSERT INTO events_archive
SELECT event_id, event_type, campaign_id, ad_request_id, creative_id, session_id,
       user_id, device, content_id, quartile, ad_pod_id, ad_slot, ad_duration_s,
       playhead_s, `source`, transport, ssai_twin_of, ingest_endpoint, server_ts,
       fill, `_late`, `_dup`,
       CAST(event_time AS TIMESTAMP(3)),
       'ad.request',
       DATE_FORMAT(event_time, 'yyyy-MM-dd'), DATE_FORMAT(event_time, 'HH')
FROM src_request;

INSERT INTO events_archive
SELECT event_id, event_type, campaign_id, ad_request_id, creative_id, session_id,
       user_id, device, content_id, quartile, ad_pod_id, ad_slot, ad_duration_s,
       playhead_s, `source`, transport, ssai_twin_of, ingest_endpoint, server_ts,
       CAST(NULL AS BOOLEAN), `_late`, `_dup`,
       CAST(event_time AS TIMESTAMP(3)),
       'user.behavior',
       DATE_FORMAT(event_time, 'yyyy-MM-dd'), DATE_FORMAT(event_time, 'HH')
FROM src_behavior;

END;
