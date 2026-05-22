USE DATABASE ROIP_DB;
USE SCHEMA GOLD;
USE WAREHOUSE ROIP_WH;

-- ── Fact: hourly order aggregates ─────────────────────────────────────
CREATE TABLE IF NOT EXISTS FACT_HOURLY_ORDERS (
    EVENT_DATE          DATE          NOT NULL,
    EVENT_HOUR          INTEGER       NOT NULL,
    CITY                VARCHAR(100)  NOT NULL,
    CATEGORY            VARCHAR(100)  NOT NULL,
    ORDER_COUNT         INTEGER,
    GROSS_REVENUE       FLOAT,
    AVG_ORDER_VALUE     FLOAT,
    TOTAL_ITEMS         INTEGER,
    UNIQUE_CUSTOMERS    INTEGER,
    AVG_DISCOUNT_PCT    FLOAT,
    PROCESSING_DATE     DATE,
    GOLD_LOADED_AT      TIMESTAMP_NTZ,
    -- Clustering = Snowflake's equivalent of partitioning
    -- Queries filtered by date and city scan fewer micro-partitions
    CONSTRAINT pk_hourly PRIMARY KEY (EVENT_DATE, EVENT_HOUR, CITY, CATEGORY)
)
CLUSTER BY (EVENT_DATE, CITY);

-- ── Fact: daily SLA scorecards ────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FACT_DAILY_SLA (
    EVENT_DATE          DATE          NOT NULL,
    CITY                VARCHAR(100)  NOT NULL,
    DELIVERY_STATUS     VARCHAR(20)   NOT NULL,   -- ON_TIME / LATE / PENDING
    ORDER_COUNT         INTEGER,
    AVG_DELIVERY_HOURS  FLOAT,
    PROCESSING_DATE     DATE,
    GOLD_LOADED_AT      TIMESTAMP_NTZ,
    CONSTRAINT pk_sla PRIMARY KEY (EVENT_DATE, CITY, DELIVERY_STATUS)
)
CLUSTER BY (EVENT_DATE);

-- ── Fact: payment health ──────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS FACT_PAYMENT_HEALTH (
    EVENT_DATE          DATE          NOT NULL,
    EVENT_TYPE          VARCHAR(50)   NOT NULL,
    EVENT_COUNT         INTEGER,
    TOTAL_AMOUNT        FLOAT,
    AVG_AMOUNT          FLOAT,
    PROCESSING_DATE     DATE,
    GOLD_LOADED_AT      TIMESTAMP_NTZ,
    CONSTRAINT pk_payments PRIMARY KEY (EVENT_DATE, EVENT_TYPE)
)
CLUSTER BY (EVENT_DATE);

-- ── Monitoring: pipeline reconciliation ───────────────────────────────
CREATE TABLE IF NOT EXISTS MONITORING.RECONCILIATION_RESULTS (
    PROCESSING_DATE     DATE          NOT NULL,
    BRONZE_COUNT        INTEGER,
    SILVER_COUNT        INTEGER,
    VARIANCE_PCT        FLOAT,
    TOLERANCE_PCT       FLOAT,
    STATUS              VARCHAR(10),  -- PASS / FAIL
    CHECKED_AT          TIMESTAMP_NTZ,
    CONSTRAINT pk_recon PRIMARY KEY (PROCESSING_DATE)
);