-- ===========================================================================
-- late_dropped — 실시간 윈도우가 버린 지각 이벤트 (Flink 가 upsert)
--
-- 새 볼륨이면 이 파일이 최초 1회 실행된다.
-- 이미 있는 볼륨은 scripts/flink-submit.sh 가 잡 제출 전에 같은 파일을 다시 적용한다 (IF NOT EXISTS).
--
-- 대사(spark/reconcile.py)가 "실시간이 버렸고 배치는 센 건수" 를 여기서 센다.
-- event_id 가 기본키라 Flink 가 재처리로 같은 이벤트를 다시 써도 한 행이다.
-- 시각 컬럼은 모두 UTC (Flink 세션 time-zone = UTC).
-- ===========================================================================
CREATE TABLE IF NOT EXISTS late_dropped (
    event_id       TEXT      PRIMARY KEY,
    kind           TEXT      NOT NULL,     -- impression | quartile | complete | click | request
    campaign_id    TEXT,
    ad_request_id  TEXT,
    event_time     TIMESTAMP NOT NULL,     -- 이벤트가 일어난 시각
    window_end     TIMESTAMP NOT NULL,     -- 이 이벤트가 들어갔어야 할 1분 창의 끝
    watermark_at   TIMESTAMP NOT NULL,     -- 도착했을 때의 워터마크 (>= window_end - 1ms)
    recorded_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_late_dropped_time ON late_dropped (event_time);
