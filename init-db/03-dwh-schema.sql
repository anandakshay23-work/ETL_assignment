-- =============================================================
-- 03-dwh-schema.sql
-- Runs against etl_dwh_db
-- Creates RAW / STAGING / DWH schemas and all tables
-- =============================================================

\connect etl_dwh_db;

-- =============================================================
-- RAW SCHEMA — Untransformed data, loaded as-is from sources
-- =============================================================
CREATE SCHEMA IF NOT EXISTS raw;

-- Raw XBRL XML data (samplexml.xml)
CREATE TABLE IF NOT EXISTS raw.xml_xbrl_data (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50),
    period            VARCHAR(10),
    revenue           VARCHAR(50),
    profit            VARCHAR(50),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw hierarchical XML data (samplexml2.xml)
CREATE TABLE IF NOT EXISTS raw.xml_hierarchical_data (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50),
    company_name      VARCHAR(200),
    revenue           VARCHAR(50),
    year              VARCHAR(20),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw JSON financials data (samplejson1.json)
CREATE TABLE IF NOT EXISTS raw.json_financials_data (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50),
    year              VARCHAR(20),
    revenue           VARCHAR(50),
    currency          VARCHAR(10),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw JSON period data (samplejson2.json)
CREATE TABLE IF NOT EXISTS raw.json_period_data (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50),
    period            VARCHAR(10),
    revenue           VARCHAR(50),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw CSV data (sample_100.csv) — all sub-tables combined
CREATE TABLE IF NOT EXISTS raw.csv_data (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50),
    profit            VARCHAR(100),
    year              VARCHAR(20),
    remarks           VARCHAR(500),
    table_index       INT,
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Raw CSV metadata (entity info from bottom of CSV)
CREATE TABLE IF NOT EXISTS raw.csv_metadata (
    id                SERIAL PRIMARY KEY,
    field_name        VARCHAR(100),
    field_value       VARCHAR(500),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =============================================================
-- STAGING SCHEMA — Cleaned, mapped, quality-checked
-- =============================================================
CREATE SCHEMA IF NOT EXISTS staging;

-- Unified staging financials (all sources merged)
CREATE TABLE IF NOT EXISTS staging.stg_financials (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50) NOT NULL,
    company_name      VARCHAR(200),
    period_type       VARCHAR(10),
    year              INT,
    revenue           NUMERIC(18,2),
    revenue_usd       NUMERIC(18,2),
    profit            NUMERIC(18,2),
    currency          VARCHAR(10),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Staging companies (for dimension loading)
CREATE TABLE IF NOT EXISTS staging.stg_companies (
    id                SERIAL PRIMARY KEY,
    company_id        VARCHAR(50) NOT NULL,
    company_name      VARCHAR(200),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Reject log table (corrupted/invalid records)
CREATE TABLE IF NOT EXISTS staging.stg_reject_log (
    id                SERIAL PRIMARY KEY,
    source_file       VARCHAR(200),
    record_data       TEXT,
    rejection_reason  VARCHAR(500),
    severity          VARCHAR(20) DEFAULT 'ERROR',
    batch_id          VARCHAR(50),
    rejected_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- =============================================================
-- DWH SCHEMA — Star Schema (Dimensions + Facts)
-- =============================================================
CREATE SCHEMA IF NOT EXISTS dwh;

-- Dimension: Company (SCD Type 2)
CREATE TABLE IF NOT EXISTS dwh.dim_company (
    dim_company_sk    SERIAL PRIMARY KEY,
    company_id        VARCHAR(50) NOT NULL,
    company_name      VARCHAR(200),
    valid_from        TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to          TIMESTAMP NOT NULL DEFAULT '9999-12-31 00:00:00',
    is_current        BOOLEAN NOT NULL DEFAULT TRUE,
    created_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create index on business key for SCD lookups
CREATE INDEX IF NOT EXISTS idx_dim_company_bk
    ON dwh.dim_company (company_id, is_current);

-- Dimension: Date (for BI analysis)
CREATE TABLE IF NOT EXISTS dwh.dim_date (
    dim_date_sk       SERIAL PRIMARY KEY,
    full_date         DATE,
    year              INT,
    quarter           INT,
    month             INT,
    month_name        VARCHAR(20),
    day_of_week       INT,
    is_weekend        BOOLEAN
);

-- Seed dim_date with relevant years
INSERT INTO dwh.dim_date (full_date, year, quarter, month, month_name, day_of_week, is_weekend)
SELECT
    d::date,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    TO_CHAR(d, 'Month'),
    EXTRACT(DOW FROM d),
    EXTRACT(DOW FROM d) IN (0, 6)
FROM generate_series('2019-01-01'::date, '2025-12-31'::date, '1 day'::interval) AS d;

-- Fact: Financials
CREATE TABLE IF NOT EXISTS dwh.fact_financials (
    fact_id           SERIAL PRIMARY KEY,
    dim_company_sk    INT REFERENCES dwh.dim_company(dim_company_sk),
    company_id        VARCHAR(50) NOT NULL,
    period_type       VARCHAR(10),
    year              INT,
    revenue           NUMERIC(18,2),
    revenue_usd       NUMERIC(18,2),
    profit            NUMERIC(18,2),
    currency          VARCHAR(10),
    source_file       VARCHAR(200),
    batch_id          VARCHAR(50),
    load_timestamp    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for fact table
CREATE INDEX IF NOT EXISTS idx_fact_company
    ON dwh.fact_financials (company_id, period_type, year);

CREATE INDEX IF NOT EXISTS idx_fact_dim_company
    ON dwh.fact_financials (dim_company_sk);


-- =============================================================
-- BI-READY VIEWS (For Apache Superset)
-- =============================================================

-- View 1: Company financials summary (denormalized)
CREATE OR REPLACE VIEW dwh.v_company_financials_summary AS
SELECT
    dc.company_id,
    dc.company_name,
    ff.period_type,
    ff.year,
    ff.revenue,
    ff.revenue_usd,
    ff.profit,
    ff.currency,
    ff.source_file,
    ff.load_timestamp
FROM dwh.fact_financials ff
JOIN dwh.dim_company dc
    ON ff.dim_company_sk = dc.dim_company_sk
    AND dc.is_current = TRUE;

-- View 2: CY vs PY revenue comparison
CREATE OR REPLACE VIEW dwh.v_revenue_comparison AS
SELECT
    dc.company_id,
    dc.company_name,
    cy.revenue_usd AS cy_revenue_usd,
    py.revenue_usd AS py_revenue_usd,
    CASE
        WHEN py.revenue_usd > 0
        THEN ROUND(((cy.revenue_usd - py.revenue_usd) / py.revenue_usd) * 100, 2)
        ELSE NULL
    END AS revenue_growth_pct,
    cy.year AS cy_year
FROM dwh.dim_company dc
LEFT JOIN dwh.fact_financials cy
    ON dc.dim_company_sk = cy.dim_company_sk AND cy.period_type = 'CY'
LEFT JOIN dwh.fact_financials py
    ON dc.dim_company_sk = py.dim_company_sk AND py.period_type = 'PY'
    AND cy.source_file = py.source_file
WHERE dc.is_current = TRUE;

-- View 3: Company change history (SCD Type 2 audit)
CREATE OR REPLACE VIEW dwh.v_company_history AS
SELECT
    dim_company_sk,
    company_id,
    company_name,
    valid_from,
    valid_to,
    is_current,
    CASE
        WHEN is_current THEN 'Active'
        ELSE 'Historical'
    END AS record_status,
    updated_at
FROM dwh.dim_company
ORDER BY company_id, valid_from;


-- Log summary
DO $$
BEGIN
    RAISE NOTICE '✅ DWH DB schema created: RAW (6 tables), STAGING (3 tables), DWH (3 tables + 3 views)';
    RAISE NOTICE '✅ dim_date seeded with dates from 2019-2025';
END $$;
