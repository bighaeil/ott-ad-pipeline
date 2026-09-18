-- ===========================================================================
-- 광고 플랫폼 로컬 스키마
--   campaigns        : Flink lookup join 대상 (캠페인 메타)
--   campaign_rates   : Spark 정산 단가 (CPM/CPC)
--   event_outbox     : 단계 3 Outbox 패턴
--   daily_settlement : 단계 5 확정 집계
--   reconciliation   : 단계 5 대사 결과
-- ===========================================================================

-- --------------------------------------------------------------- campaigns
CREATE TABLE IF NOT EXISTS campaigns (
    campaign_id   TEXT PRIMARY KEY,
    advertiser    TEXT        NOT NULL,
    name          TEXT        NOT NULL,
    vertical      TEXT        NOT NULL,
    daily_budget  NUMERIC(14,2) NOT NULL DEFAULT 0,
    active        BOOLEAN     NOT NULL DEFAULT TRUE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------- campaign_rates
CREATE TABLE IF NOT EXISTS campaign_rates (
    campaign_id   TEXT PRIMARY KEY REFERENCES campaigns(campaign_id),
    cpm           NUMERIC(12,2) NOT NULL,   -- 1000 임프레션당 단가 (KRW)
    cpc           NUMERIC(12,2) NOT NULL DEFAULT 0,
    currency      TEXT          NOT NULL DEFAULT 'KRW'
);

-- ------------------------------------------------------------ event_outbox
-- ad-decision 서비스가 "소재 결정 응답"과 같은 트랜잭션에서 INSERT 한다.
-- Outbox 워커가 published=false 를 폴링해 Kafka ad.request 로 발행.
CREATE TABLE IF NOT EXISTS event_outbox (
    id            BIGSERIAL PRIMARY KEY,
    event_id      TEXT        NOT NULL,
    event_type    TEXT        NOT NULL,
    aggregate_id  TEXT        NOT NULL,          -- ad_request_id (= Kafka 파티션 키)
    payload       JSONB       NOT NULL,
    occurred_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    published     BOOLEAN     NOT NULL DEFAULT FALSE,
    published_at  TIMESTAMPTZ
);
-- event_id 에 UNIQUE 를 걸지 않는다.
-- 워커가 발행 후 플래그 업데이트에 실패하면 재발행되어 중복이 생기는 것이
-- 이 실습에서 관찰하려는 "의도된 동작"이기 때문.
CREATE INDEX IF NOT EXISTS idx_outbox_unpublished
    ON event_outbox (id) WHERE published = FALSE;
CREATE INDEX IF NOT EXISTS idx_outbox_occurred
    ON event_outbox (occurred_at);

-- -------------------------------------------------------- daily_settlement
CREATE TABLE IF NOT EXISTS daily_settlement (
    dt              DATE   NOT NULL,
    campaign_id     TEXT   NOT NULL,
    requests        BIGINT NOT NULL DEFAULT 0,
    impressions     BIGINT NOT NULL DEFAULT 0,
    clicks          BIGINT NOT NULL DEFAULT 0,
    completes       BIGINT NOT NULL DEFAULT 0,
    raw_impressions BIGINT NOT NULL DEFAULT 0,  -- 중복 제거 전 (SSAI 이중경로 포함)
    cpm             NUMERIC(12,2) NOT NULL DEFAULT 0,
    amount          NUMERIC(16,2) NOT NULL DEFAULT 0,
    computed_at     TIMESTAMPTZ   NOT NULL DEFAULT now(),
    PRIMARY KEY (dt, campaign_id)
);

-- ---------------------------------------------------------- reconciliation
CREATE TABLE IF NOT EXISTS reconciliation (
    id                  BIGSERIAL PRIMARY KEY,
    run_at              TIMESTAMPTZ NOT NULL DEFAULT now(),
    dt                  DATE        NOT NULL,
    campaign_id         TEXT        NOT NULL,
    realtime_impressions BIGINT     NOT NULL,   -- Redis 분단위 합계
    batch_impressions    BIGINT     NOT NULL,   -- daily_settlement 확정값
    diff                 BIGINT     NOT NULL,
    diff_rate            NUMERIC(8,4) NOT NULL,
    likely_cause         TEXT
);
CREATE INDEX IF NOT EXISTS idx_recon_run ON reconciliation (run_at DESC);

-- =============================== 시드 데이터 ===============================
INSERT INTO campaigns (campaign_id, advertiser, name, vertical, daily_budget) VALUES
    ('cmp-1001', '한빛통신',   '5G 무제한 요금제',   'telco',     5000000),
    ('cmp-1002', '서울은행',   '첫거래 우대적금',     'finance',   3000000),
    ('cmp-1003', '델타모터스', '전기 SUV 사전예약',   'auto',      8000000),
    ('cmp-1004', '누리커머스', '가을 정기세일',       'commerce',  2000000),
    ('cmp-1005', '한강식품',   '냉동간편식 신제품',   'fmcg',      1500000)
ON CONFLICT (campaign_id) DO NOTHING;

INSERT INTO campaign_rates (campaign_id, cpm, cpc) VALUES
    ('cmp-1001', 9500.00,  450.00),
    ('cmp-1002', 12000.00, 700.00),
    ('cmp-1003', 15000.00, 900.00),
    ('cmp-1004', 7000.00,  300.00),
    ('cmp-1005', 6000.00,  250.00)
ON CONFLICT (campaign_id) DO NOTHING;
