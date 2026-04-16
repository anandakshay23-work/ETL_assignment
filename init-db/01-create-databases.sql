-- =============================================================
-- 01-create-databases.sql
-- Creates the DWH database (config DB is auto-created by Docker)
-- =============================================================

-- Create the Data Warehouse database
CREATE DATABASE etl_dwh_db OWNER etl_user;

-- Grant full privileges
GRANT ALL PRIVILEGES ON DATABASE etl_dwh_db TO etl_user;
GRANT ALL PRIVILEGES ON DATABASE etl_config_db TO etl_user;
