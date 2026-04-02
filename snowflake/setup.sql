-- ============================================
-- FRED Financial Pipeline — Snowflake Setup
-- Run this in a Snowflake worksheet to create
-- the database, schemas, warehouse, and raw tables.
-- ============================================

-- Database
CREATE DATABASE IF NOT EXISTS FRED_PIPELINE;
USE DATABASE FRED_PIPELINE;

-- Schemas (one per dbt layer)
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS INTERMEDIATE;
CREATE SCHEMA IF NOT EXISTS MARTS;

-- Warehouse (XSmall, auto-suspend after 60s to save credits)
CREATE WAREHOUSE IF NOT EXISTS FRED_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

USE WAREHOUSE FRED_WH;
USE SCHEMA RAW;

-- Raw observations — where FRED API data lands
CREATE TABLE IF NOT EXISTS RAW.FRED_OBSERVATIONS (
    series_id         VARCHAR(20)       NOT NULL,
    observation_date  DATE              NOT NULL,
    value             FLOAT,
    ingested_at       TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP(),
    source_url        VARCHAR(500)
);

-- Series metadata — describes each economic indicator
CREATE TABLE IF NOT EXISTS RAW.FRED_SERIES_METADATA (
    series_id         VARCHAR(20)       NOT NULL,
    title             VARCHAR(500),
    frequency         VARCHAR(20),
    units             VARCHAR(200),
    seasonal_adj      VARCHAR(200),
    last_updated      TIMESTAMP_NTZ,
    ingested_at       TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP()
);

-- Ingestion log — tracks every pipeline run for monitoring
CREATE TABLE IF NOT EXISTS RAW.INGESTION_LOG (
    run_id            VARCHAR(36)       NOT NULL,
    series_id         VARCHAR(20)       NOT NULL,
    records_fetched   INTEGER           DEFAULT 0,
    status            VARCHAR(20)       NOT NULL,
    started_at        TIMESTAMP_NTZ     NOT NULL,
    completed_at      TIMESTAMP_NTZ,
    error_message     VARCHAR(2000)
);

-- Verify setup
SELECT 'FRED_OBSERVATIONS' AS table_name, COUNT(*) AS row_count 
FROM RAW.FRED_OBSERVATIONS
UNION ALL
SELECT 'FRED_SERIES_METADATA', COUNT(*) FROM RAW.FRED_SERIES_METADATA
UNION ALL
SELECT 'INGESTION_LOG', COUNT(*) FROM RAW.INGESTION_LOG;
