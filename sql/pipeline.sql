-- ===========================================================================
-- 실시간 처리 파이프라인 (Flink SQL)
--
-- 실행:  bash scripts/flink-submit.sh
--        (스크립트가 __WATERMARK_DELAY__ 등 자리표시자를 환경변수로 치환한 뒤
--         sql-client.sh -f 로 넘긴다. SQL Client 자체는 변수 치환을 못 한다.)
--
-- 하나의 잡에 모든 싱크를 담는다 (EXECUTE STATEMENT SET).
-- 로컬 슬롯이 2개뿐이라 잡을 여러 개 띄우면 자리가 없다.
-- ===========================================================================

-- ------------------------------------------------------------- 세션 설정
SET 'pipeline.name' = 'ott-ads-realtime';

-- 로컬 1 / 운영 20 (TM 5대 x 4슬롯)
SET 'parallelism.default' = '1';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.min-pause' = '5s';

-- event_id 중복제거 상태의 수명. 1시간이 지난 event_id 는 잊는다.
-- = 1시간 뒤에 도착한 재전송은 중복으로 못 잡는다는 뜻이다.
-- 그 구멍을 단계 5의 Spark 배치가 전체 범위 중복제거로 메운다.
SET 'table.exec.state.ttl' = '1 h';

SET 'table.local-time-zone' = 'UTC';

-- 연산자 체이닝. 관찰이 목적이라 기본을 false 로 둔다.
-- true 면 8개 연산자가 하나의 vertex 로 뭉쳐서 "중복제거가 몇 건을 걷어냈는지" 를
-- Flink REST 로 볼 수 없다. false 면 vertex 별 read-records/write-records 가 노출되어
-- 대시보드가 [처리] 패널을 그릴 수 있다. 대신 연산자 사이 직렬화 비용이 조금 든다.
SET 'pipeline.operator-chaining' = '__CHAINING__';

-- 유휴 파티션 처리. 이게 없으면 윈도우가 안 닫힌다.
--   워터마크는 모든 입력(4개 토픽 x 6파티션)의 최솟값이다.
--   ad.click 처럼 트래픽이 희박한 토픽은 어떤 파티션에 한동안 데이터가 없는데,
--   그 파티션의 워터마크가 과거에 머물면서 전체 워터마크를 붙잡는다.
--   결과: 이벤트는 계속 들어오는데 집계 결과가 한 건도 안 나온다.
-- 이 시간 동안 데이터가 없는 파티션은 "유휴"로 표시해 워터마크 계산에서 뺀다.
SET 'table.exec.source.idle-timeout' = '__IDLE_TIMEOUT__ s';

-- 같은 소스를 여러 싱크가 읽으므로 소스 재사용을 켠다 (기본값이지만 명시).
SET 'table.optimizer.reuse-source-enabled' = 'true';

-- 공통 부분(중복 제거 등)을 싱크들이 하나로 공유하게 한다.
-- 이게 없으면 옵티마이저가 싱크마다 안 쓰는 컬럼을 먼저 잘라내서 모양이 달라지고,
-- 같은 중복 제거가 싱크 경로마다 따로 생긴다 (상태가 두 배, 대시보드 계수도 두 배).
SET 'table.optimizer.reuse-optimize-block-with-digest-enabled' = 'true';

-- ===========================================================================
-- 1. 소스 테이블
--    event_time 을 시간 속성으로 삼고 워터마크를 건다.
--    워터마크 지연은 __WATERMARK_DELAY__ 초 (기본 10, FLINK_WATERMARK_DELAY 로 변경).
--    이 값이 작으면 late.events 가 늘고, 크면 집계가 늦게 나온다.
-- ===========================================================================

CREATE TABLE impression_raw (
    event_id       STRING,
    event_type     STRING,
    campaign_id    STRING,
    ad_request_id  STRING,
    creative_id    STRING,
    session_id     STRING,
    `source`       STRING,
    transport      STRING,
    ssai_twin_of   STRING,
    event_time     TIMESTAMP_LTZ(3),
    server_ts      STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '__WATERMARK_DELAY__' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad.impression',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-rt',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE quartile_raw (
    event_id       STRING,
    event_type     STRING,
    campaign_id    STRING,
    ad_request_id  STRING,
    quartile       STRING,
    `source`       STRING,
    event_time     TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '__WATERMARK_DELAY__' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad.quartile',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-rt',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

CREATE TABLE click_raw (
    event_id       STRING,
    event_type     STRING,
    campaign_id    STRING,
    ad_request_id  STRING,
    `source`       STRING,
    event_time     TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '__WATERMARK_DELAY__' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad.click',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-rt',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

-- ad.request 에는 두 갈래가 섞여 들어온다.
--   event_type = 'ad_request'  : 클라이언트가 본 사실 (Collector 경유, campaign_id='pending')
--   event_type = 'ad_response' : 서버가 확정한 사실 (Outbox 경유, campaign_id 실값 + fill)
-- 정합성 지표는 서버 확정분(ad_response, fill=true)을 분모로 쓴다.
CREATE TABLE request_raw (
    event_id       STRING,
    event_type     STRING,
    campaign_id    STRING,
    ad_request_id  STRING,
    fill           BOOLEAN,
    `source`       STRING,
    transport      STRING,
    event_time     TIMESTAMP_LTZ(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '__WATERMARK_DELAY__' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'ad.request',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'flink-rt',
    'scan.startup.mode' = '__STARTUP_MODE__',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601',
    'json.ignore-parse-errors' = 'true'
);

-- ===========================================================================
-- 2. 싱크 테이블
-- ===========================================================================

-- 분단위 집계 결과.
-- 원래는 여기서 바로 Redis 에 써야 하지만 Flink 1.20 용 Redis SQL 커넥터가 없다.
-- (flink/Dockerfile 주석 참조) Kafka 로 내보내고 redis-writer 가 Redis 에 적재한다.
CREATE TABLE agg_minute_sink (
    window_start   TIMESTAMP_LTZ(3),
    window_end     TIMESTAMP_LTZ(3),
    campaign_id    STRING,
    advertiser     STRING,
    campaign_name  STRING,
    impressions    BIGINT,
    clicks         BIGINT,
    completes      BIGINT,
    requests       BIGINT,
    imp_req_ratio  DOUBLE,
    ssai_dupes     BIGINT,
    emitted_at     TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'agg.minute',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 윈도우가 이미 닫힌 뒤 도착해 실시간 집계에서 버려진 이벤트 (정의는 7-3 참고).
CREATE TABLE late_events_sink (
    event_id       STRING,
    event_type     STRING,
    campaign_id    STRING,
    ad_request_id  STRING,
    event_time     TIMESTAMP_LTZ(3),
    window_end     TIMESTAMP_LTZ(3),
    watermark_at   TIMESTAMP_LTZ(3),
    lateness_ms    BIGINT,
    topic_origin   STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'late.events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 같은 지각 이벤트를 대사용으로 PostgreSQL 에도 남긴다.
-- Kafka 쪽(redis-writer 의 late 카운터)은 재처리하면 두 번 세지만,
-- 여기는 event_id 가 기본키라 같은 이벤트를 몇 번 써도 한 행이다 (upsert).
-- 그래서 대사(spark/reconcile.py)는 이 테이블로 "실시간이 버린 건수" 를 정확히 센다.
-- JDBC 커넥터가 TIMESTAMP_LTZ 를 못 받으므로 UTC 기준 TIMESTAMP 로 바꿔 쓴다
-- (세션 time-zone 이 UTC 라 값이 그대로 UTC 다).
CREATE TABLE late_dropped_sink (
    event_id       STRING,
    kind           STRING,
    campaign_id    STRING,
    ad_request_id  STRING,
    event_time     TIMESTAMP(3),
    window_end     TIMESTAMP(3),
    watermark_at   TIMESTAMP(3),
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/adplatform',
    'table-name' = 'late_dropped',
    'username' = 'ads',
    'password' = 'ads',
    'sink.buffer-flush.max-rows' = '500',
    'sink.buffer-flush.interval' = '1s'
);

-- 이상 감지 경고.
CREATE TABLE alert_sink (
    alert_type     STRING,
    campaign_id    STRING,
    window_start   TIMESTAMP_LTZ(3),
    window_end     TIMESTAMP_LTZ(3),
    impressions    BIGINT,
    requests       BIGINT,
    imp_req_ratio  DOUBLE,
    threshold      DOUBLE,
    message        STRING,
    detected_at    TIMESTAMP_LTZ(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'alert.anomaly',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'ISO-8601'
);

-- 캠페인 메타. lookup join 대상 (차원 테이블).
-- 브로드캐스트 대신 lookup 을 고른 이유: 캠페인이 5개뿐이고 갱신이 드물어
-- 부분 캐시(5분)만으로 DB 부하가 사실상 0 이 된다.
CREATE TABLE campaigns (
    campaign_id  STRING,
    advertiser   STRING,
    `name`       STRING,
    vertical     STRING,
    PRIMARY KEY (campaign_id) NOT ENFORCED
) WITH (
    'connector' = 'jdbc',
    'url' = 'jdbc:postgresql://postgres:5432/adplatform',
    'table-name' = 'campaigns',
    'username' = 'ads',
    'password' = 'ads',
    'lookup.cache' = 'PARTIAL',
    'lookup.partial-cache.max-rows' = '500',
    'lookup.partial-cache.expire-after-write' = '5 min',
    'lookup.max-retries' = '2'
);

-- ===========================================================================
-- 3. 네 소스를 하나로 합친다.
--    이렇게 해야 중복제거 연산자와 윈도우 집계가 각각 하나로 끝난다.
--    (소스마다 따로 하면 슬롯 2개로는 감당이 안 된다)
-- ===========================================================================
CREATE TEMPORARY VIEW all_events AS
SELECT event_id, campaign_id, ad_request_id, event_time,
       'impression' AS kind,
       CASE WHEN ssai_twin_of IS NOT NULL THEN 1 ELSE 0 END AS is_ssai_twin
FROM impression_raw
WHERE campaign_id IS NOT NULL

UNION ALL
SELECT event_id, campaign_id, ad_request_id, event_time,
       CASE WHEN quartile = 'complete' THEN 'complete' ELSE 'quartile' END AS kind,
       0 AS is_ssai_twin
FROM quartile_raw
WHERE campaign_id IS NOT NULL

UNION ALL
SELECT event_id, campaign_id, ad_request_id, event_time,
       'click' AS kind, 0 AS is_ssai_twin
FROM click_raw
WHERE campaign_id IS NOT NULL

UNION ALL
-- 서버가 확정해 실제로 채운 광고만 분모로 센다.
-- 노필(fill=false)과 클라이언트측 ad_request(campaign_id='pending')는 제외.
SELECT event_id, campaign_id, ad_request_id, event_time,
       'request' AS kind, 0 AS is_ssai_twin
FROM request_raw
WHERE event_type = 'ad_response' AND fill = TRUE AND campaign_id <> 'nofill';

-- ---------------------------------------------------------------------------
-- 4. event_id 기준 중복 제거 (첫 번째 행만 남긴다)
--
--    이게 잡는 것   : 생성기의 dup 재전송, Outbox 워커의 재발행
--    이게 못 잡는 것: SSAI 이중경로 (event_id 가 서로 다르다)
--    후자는 단계 5의 Spark 가 ad_request_id + source 우선순위로 처리한다.
--
--    "먼저 도착한 것" (처리 시간 순) 을 남긴다. event_time 순으로 정렬하면
--    나중에 더 이른 event_time 이 오면 앞서 낸 결과를 고쳐야 하므로 출력이 수정 스트림이 되고,
--    추가만 받는 싱크(late.events, 5-1)가 이 뒤에 붙을 수 없다.
--    중복은 재전송·재발행이라 event_time 이 같으므로 어느 쪽을 남겨도 값은 같다.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY VIEW deduped AS
SELECT event_id, campaign_id, ad_request_id, event_time, kind, is_ssai_twin
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY pt ASC) AS rn
    FROM (SELECT *, PROCTIME() AS pt FROM all_events)
)
WHERE rn = 1;

-- ---------------------------------------------------------------------------
-- 5. 1분 텀블링 윈도우 집계
-- ---------------------------------------------------------------------------
CREATE TEMPORARY VIEW agg_1m AS
SELECT
    window_start,
    window_end,
    campaign_id,
    COUNT(*) FILTER (WHERE kind = 'impression')             AS impressions,
    COUNT(*) FILTER (WHERE kind = 'click')                  AS clicks,
    COUNT(*) FILTER (WHERE kind = 'complete')               AS completes,
    COUNT(*) FILTER (WHERE kind = 'request')                AS requests,
    CAST(SUM(is_ssai_twin) AS BIGINT)                       AS ssai_dupes
FROM TABLE(TUMBLE(TABLE deduped, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, campaign_id;

-- ---------------------------------------------------------------------------
-- 5-1. 윈도우가 버리는 이벤트 = 지각 이벤트
--
--    윈도우 집계(5)는 "그 이벤트가 속한 1분 창이 이미 닫혔으면" 조용히 버린다.
--    창이 닫혔다 = 워터마크가 창 끝(window_end) - 1ms 에 도달했다.
--    그래서 지각 판정도 event_time < 워터마크 가 아니라 이 조건이어야 한다.
--      예) 워터마크 12:00:40 에 event_time 12:00:30 이 도착
--          event_time < 워터마크 이지만 [12:00, 12:01) 창은 아직 열려 있다 -> 실시간에 집계된다.
--          예전 판정(event_time < 워터마크)은 이것을 지각으로 셌고, 대사 잔차가 음수로 나왔다.
--
--    중복 제거(4) 뒤에서 판정한다. 윈도우가 보는 것과 같은 레코드, 같은 워터마크를 보기 위해서다.
--    (앞에서 판정하면 중복 재전송도 지각으로 두 번 세고, 워터마크도 윈도우와 다르다)
--    TUMBLE 을 쓰지 않고 창 끝을 직접 계산한다. TUMBLE 을 쓰면 이 연산자도 지각 이벤트를 버린다.
-- ---------------------------------------------------------------------------
--    FLOOR 가 TIMESTAMP_LTZ 를 받지 못해 TIMESTAMP(3) 로 바꿔 계산한다. 세션 time-zone 이 UTC 라
--    값은 그대로 UTC 이고, window_end / watermark_ts 는 UTC 기준 TIMESTAMP(3) 다.
CREATE TEMPORARY VIEW late_at_window AS
SELECT event_id, kind, campaign_id, ad_request_id, event_time, window_end, watermark_ts
FROM (
    SELECT *,
           FLOOR(CAST(event_time AS TIMESTAMP(3)) TO MINUTE) + INTERVAL '1' MINUTE AS window_end,
           CAST(CURRENT_WATERMARK(event_time) AS TIMESTAMP(3))                     AS watermark_ts
    FROM deduped
)
WHERE watermark_ts IS NOT NULL
  AND watermark_ts >= window_end - INTERVAL '0.001' SECOND;

-- lookup join 은 처리시간 속성을 요구한다. 집계 결과 위에 한 겹 더 얹어 만든다.
-- (집계 SELECT 목록에 PROCTIME() 을 직접 넣으면 GROUP BY 규칙에 걸린다)
CREATE TEMPORARY VIEW agg_1m_pt AS
SELECT *, PROCTIME() AS pt FROM agg_1m;

-- ---------------------------------------------------------------------------
-- 6. 캠페인 메타 lookup join
--    윈도우 집계 뒤에 붙인다. 이벤트마다 조인하면 초당 수천 번 DB 를 두드리지만,
--    분단위 결과에 붙이면 캠페인 수만큼(5회/분)이면 끝난다.
-- ---------------------------------------------------------------------------
CREATE TEMPORARY VIEW agg_1m_enriched AS
SELECT
    a.window_start,
    a.window_end,
    a.campaign_id,
    c.advertiser,
    c.`name` AS campaign_name,
    a.impressions,
    a.clicks,
    a.completes,
    a.requests,
    CASE WHEN a.requests > 0
         THEN CAST(a.impressions AS DOUBLE) / CAST(a.requests AS DOUBLE)
         ELSE CAST(NULL AS DOUBLE) END AS imp_req_ratio,
    a.ssai_dupes
FROM agg_1m_pt AS a
LEFT JOIN campaigns FOR SYSTEM_TIME AS OF a.pt AS c
     ON a.campaign_id = c.campaign_id;

-- ===========================================================================
-- 7. 모든 싱크를 하나의 잡으로 실행
-- ===========================================================================
EXECUTE STATEMENT SET
BEGIN

-- 7-1. 분단위 집계 -> agg.minute (-> redis-writer -> Redis)
INSERT INTO agg_minute_sink
SELECT window_start, window_end, campaign_id, advertiser, campaign_name,
       impressions, clicks, completes, requests, imp_req_ratio, ssai_dupes,
       CURRENT_TIMESTAMP AS emitted_at
FROM agg_1m_enriched;

-- 7-2. 이상 감지: impression/request 비율이 임계치 밑
--      노필과 별개로, "채우기로 결정했는데 실제로 안 나온" 비율이 떨어지면 경고.
--      캠페인 메타(광고주·이름)를 쓰지 않으므로 lookup join 전의 agg_1m 을 바로 읽는다.
--      (조인이 경로마다 하나씩 생기면 campaigns 스캔을 공유하게 되어 lookup join 이 성립하지 않는다)
INSERT INTO alert_sink
SELECT
    'low_impression_ratio' AS alert_type,
    campaign_id,
    window_start,
    window_end,
    impressions,
    requests,
    CAST(impressions AS DOUBLE) / CAST(requests AS DOUBLE) AS imp_req_ratio,
    CAST(__ALERT_THRESHOLD__ AS DOUBLE) AS threshold,
    'impression/request 비율이 임계치 미만' AS message,
    CURRENT_TIMESTAMP AS detected_at
FROM agg_1m
WHERE requests >= __ALERT_MIN_REQUESTS__
  AND CAST(impressions AS DOUBLE) / CAST(requests AS DOUBLE) < __ALERT_THRESHOLD__;

-- 7-3. 지각 이벤트 (5-1) -> late.events (관찰용) + late_dropped (대사용)
--      Flink SQL 윈도우에는 사이드 아웃풋이 없다. 그래서 윈도우가 버리는 조건을
--      CURRENT_WATERMARK() 로 똑같이 재현해 따로 뽑아낸다.
INSERT INTO late_events_sink
SELECT event_id,
       kind AS event_type,
       campaign_id, ad_request_id, event_time,
       CAST(window_end AS TIMESTAMP_LTZ(3))   AS window_end,
       CAST(watermark_ts AS TIMESTAMP_LTZ(3)) AS watermark_at,
       TIMESTAMPDIFF(SECOND, CAST(event_time AS TIMESTAMP(3)), watermark_ts) * 1000 AS lateness_ms,
       CASE kind WHEN 'impression' THEN 'ad.impression'
                 WHEN 'click'      THEN 'ad.click'
                 WHEN 'request'    THEN 'ad.request'
                 ELSE 'ad.quartile' END AS topic_origin
FROM late_at_window;

INSERT INTO late_dropped_sink
SELECT event_id, kind, campaign_id, ad_request_id,
       CAST(event_time AS TIMESTAMP(3)), window_end, watermark_ts
FROM late_at_window;

END;
