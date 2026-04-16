-- =============================================================
-- 02-config-schema.sql
-- Runs against etl_config_db (default DB)
-- Creates mapping table + seeds from mapping_xbrl.csv data
-- =============================================================

\connect etl_config_db;

-- Mapping configuration table (database-driven mapping)
CREATE TABLE IF NOT EXISTS mapping_xbrl (
    id              SERIAL PRIMARY KEY,
    source_field    VARCHAR(100) NOT NULL,
    target_column   VARCHAR(100) NOT NULL,
    transformation  VARCHAR(100) NOT NULL,
    datatype        VARCHAR(50)  NOT NULL,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Seed mapping data from mapping_xbrl.csv
INSERT INTO mapping_xbrl (source_field, target_column, transformation, datatype) VALUES
    ('ID',       'company_id',   'direct',           'varchar'),
    ('Name',     'company_name', 'upper',            'varchar'),
    ('Revenue',  'revenue',      'currency_convert', 'numeric'),
    ('Profit',   'profit',       'split_comma',      'numeric'),
    ('Year',     'year',         'direct',           'int'),
    ('Currency', 'currency',     'direct',           'varchar'),
    ('Period',   'period_type',  'map_cy_py',        'varchar');

-- Pipeline metadata table (tracks pipeline runs)
CREATE TABLE IF NOT EXISTS pipeline_metadata (
    run_id          VARCHAR(50) PRIMARY KEY,
    start_time      TIMESTAMP NOT NULL,
    end_time        TIMESTAMP,
    status          VARCHAR(20) DEFAULT 'RUNNING',
    records_processed INT DEFAULT 0,
    records_rejected  INT DEFAULT 0,
    source_files    TEXT,
    error_message   TEXT,
    created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Log summary
DO $$
BEGIN
    RAISE NOTICE '✅ Config DB schema created: mapping_xbrl (% rows seeded), pipeline_metadata',
        (SELECT COUNT(*) FROM mapping_xbrl);
END $$;
