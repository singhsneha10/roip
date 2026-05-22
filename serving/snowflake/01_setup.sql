-- Run this once to set up the Snowflake environment
-- ── Database and schema ───────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS ROIP_DB;
USE DATABASE ROIP_DB;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;
CREATE SCHEMA IF NOT EXISTS MONITORING;

-- ── Warehouse (compute cluster) ───────────────────────────────────────
-- AUTO_SUSPEND = pauses after 60s idle → you only pay when queries run
-- AUTO_RESUME  = starts automatically when a query hits it
CREATE WAREHOUSE IF NOT EXISTS ROIP_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND   = 60
    AUTO_RESUME    = TRUE
    COMMENT        = 'ROIP Gold query warehouse';

-- ── Roles ─────────────────────────────────────────────────────────────
CREATE ROLE IF NOT EXISTS ROIP_LOADER;    -- pipeline writes Gold
CREATE ROLE IF NOT EXISTS ROIP_ANALYST;   -- BI tools read Gold
CREATE ROLE IF NOT EXISTS ROIP_ADMIN;     -- full access

-- Grant warehouse to roles
GRANT USAGE ON WAREHOUSE ROIP_WH TO ROLE ROIP_LOADER;
GRANT USAGE ON WAREHOUSE ROIP_WH TO ROLE ROIP_ANALYST;

-- Grant schema access
GRANT USAGE ON DATABASE ROIP_DB TO ROLE ROIP_LOADER;
GRANT ALL   ON SCHEMA ROIP_DB.GOLD       TO ROLE ROIP_LOADER;
GRANT USAGE ON DATABASE ROIP_DB TO ROLE ROIP_ANALYST;
GRANT SELECT ON ALL TABLES IN SCHEMA ROIP_DB.GOLD TO ROLE ROIP_ANALYST;
GRANT SELECT ON FUTURE TABLES IN SCHEMA ROIP_DB.GOLD TO ROLE ROIP_ANALYST;