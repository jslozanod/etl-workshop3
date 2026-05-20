-- ============================================================
-- Workshop 3 — ETL Streaming Pipeline
-- Database Schema: Happiness Predictions
-- Database: PostgreSQL 15
-- ============================================================

-- ─── Drop existing tables (safe re-run) ─────────────────────
DROP TABLE IF EXISTS fact_predictions     CASCADE;
DROP TABLE IF EXISTS dim_raw_event        CASCADE;
DROP TABLE IF EXISTS raw_happiness_events CASCADE;
DROP TABLE IF EXISTS dim_country          CASCADE;
DROP TABLE IF EXISTS dim_date             CASCADE;

-- ============================================================
-- RAW LAYER
-- Stores every Kafka message exactly as received.
-- Supports traceability, auditing, and future reprocessing.
-- ============================================================
CREATE TABLE raw_happiness_events (
    raw_event_id       SERIAL PRIMARY KEY,
    received_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Original Kafka payload fields
    country            TEXT,
    year               INTEGER,
    gdp                DOUBLE PRECISION,
    family             DOUBLE PRECISION,
    health             DOUBLE PRECISION,
    freedom            DOUBLE PRECISION,
    generosity         DOUBLE PRECISION,
    corruption         DOUBLE PRECISION,
    actual_happiness_score DOUBLE PRECISION,

    -- Raw JSON payload for full traceability
    raw_payload        TEXT,

    -- Processing status applied by the consumer
    -- VALID | INVALID_SCHEMA | INVALID_VALUES | PREDICTION_ERROR
    processing_status  VARCHAR(30) NOT NULL DEFAULT 'VALID',

    -- Validation details if status != VALID
    validation_message TEXT
);

-- ============================================================
-- DIMENSION: Raw Event
-- Analytical view of the raw event for dimensional modeling.
-- Populated by the consumer after inserting into raw_happiness_events.
-- ============================================================
CREATE TABLE dim_raw_event (
    dim_raw_event_id       SERIAL PRIMARY KEY,
    raw_event_id           INTEGER NOT NULL REFERENCES raw_happiness_events(raw_event_id),
    country                TEXT,
    year                   INTEGER,
    gdp                    DOUBLE PRECISION,
    family                 DOUBLE PRECISION,
    health                 DOUBLE PRECISION,
    freedom                DOUBLE PRECISION,
    generosity             DOUBLE PRECISION,
    corruption             DOUBLE PRECISION,
    actual_happiness_score DOUBLE PRECISION,
    processing_status      VARCHAR(30),
    received_at            TIMESTAMP WITH TIME ZONE
);

-- ============================================================
-- DIMENSION: Country
-- ============================================================
CREATE TABLE dim_country (
    country_id   SERIAL PRIMARY KEY,
    country_name TEXT NOT NULL UNIQUE,
    region       TEXT
);

-- ============================================================
-- DIMENSION: Date
-- ============================================================
CREATE TABLE dim_date (
    date_id      SERIAL PRIMARY KEY,
    year         INTEGER NOT NULL,
    UNIQUE (year)
);

-- ============================================================
-- FACT TABLE: Predictions
-- One row per valid streaming event that produced a prediction.
-- ============================================================
CREATE TABLE fact_predictions (
    prediction_id        SERIAL PRIMARY KEY,
    raw_event_id         INTEGER NOT NULL REFERENCES raw_happiness_events(raw_event_id),
    dim_raw_event_id     INTEGER REFERENCES dim_raw_event(dim_raw_event_id),
    country_id           INTEGER REFERENCES dim_country(country_id),
    date_id              INTEGER REFERENCES dim_date(date_id),

    -- Scores
    actual_score         DOUBLE PRECISION NOT NULL,
    predicted_score      DOUBLE PRECISION NOT NULL,
    prediction_error     DOUBLE PRECISION GENERATED ALWAYS AS (ABS(actual_score - predicted_score)) STORED,

    -- Feature snapshot at prediction time
    gdp_per_capita       DOUBLE PRECISION,
    social_support       DOUBLE PRECISION,
    health_life_expectancy DOUBLE PRECISION,
    freedom              DOUBLE PRECISION,
    generosity           DOUBLE PRECISION,
    corruption           DOUBLE PRECISION,

    -- Audit
    prediction_timestamp TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- ============================================================
-- INDEXES for dashboard query performance
-- ============================================================
CREATE INDEX idx_fact_country  ON fact_predictions(country_id);
CREATE INDEX idx_fact_date     ON fact_predictions(date_id);
CREATE INDEX idx_fact_ts       ON fact_predictions(prediction_timestamp);
CREATE INDEX idx_raw_status    ON raw_happiness_events(processing_status);

-- ============================================================
-- SEED: dim_date (years in the dataset)
-- ============================================================
INSERT INTO dim_date (year) VALUES (2015),(2016),(2017),(2018),(2019)
ON CONFLICT (year) DO NOTHING;

-- ============================================================
-- VIEW: KPI helper view for dashboard
-- ============================================================
CREATE OR REPLACE VIEW vw_prediction_kpis AS
SELECT
    dc.country_name,
    dd.year,
    fp.actual_score,
    fp.predicted_score,
    fp.prediction_error,
    fp.prediction_timestamp,
    fp.gdp_per_capita,
    fp.social_support,
    fp.health_life_expectancy,
    fp.freedom,
    fp.generosity,
    fp.corruption
FROM fact_predictions fp
LEFT JOIN dim_country dc ON fp.country_id  = dc.country_id
LEFT JOIN dim_date    dd ON fp.date_id     = dd.date_id;
