-- ============================================================
-- Workshop 3 — KPI Queries
-- These queries power the Streamlit dashboard.
-- ============================================================

-- ─── KPI 1: Average prediction error (overall and by year) ──
SELECT
    dd.year,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_prediction_error,
    ROUND(MIN(fp.prediction_error)::NUMERIC, 4) AS min_error,
    ROUND(MAX(fp.prediction_error)::NUMERIC, 4) AS max_error,
    COUNT(*)                                     AS total_predictions
FROM fact_predictions fp
LEFT JOIN dim_date dd ON fp.date_id = dd.date_id
GROUP BY dd.year
ORDER BY dd.year;

-- ─── KPI 2: Predictions by country (top 20 by count) ─────────
SELECT
    dc.country_name,
    COUNT(*)                                     AS total_predictions,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4)     AS avg_actual_score,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4)  AS avg_predicted_score,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4) AS avg_error
FROM fact_predictions fp
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
GROUP BY dc.country_name
ORDER BY total_predictions DESC
LIMIT 20;

-- ─── KPI 3: Predicted vs Actual Score (full dataset) ─────────
SELECT
    dc.country_name,
    dd.year,
    fp.actual_score,
    fp.predicted_score,
    fp.prediction_error,
    fp.prediction_timestamp
FROM fact_predictions fp
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
LEFT JOIN dim_date    dd ON fp.date_id    = dd.date_id
ORDER BY fp.prediction_timestamp;

-- ─── KPI 4: Prediction trends over time ───────────────────────
SELECT
    DATE_TRUNC('minute', fp.prediction_timestamp) AS prediction_minute,
    COUNT(*)                                        AS events_processed,
    ROUND(AVG(fp.actual_score)::NUMERIC, 4)        AS avg_actual,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4)     AS avg_predicted,
    ROUND(AVG(fp.prediction_error)::NUMERIC, 4)    AS avg_error
FROM fact_predictions fp
GROUP BY DATE_TRUNC('minute', fp.prediction_timestamp)
ORDER BY prediction_minute;

-- ─── KPI 5: Raw event processing summary (data quality audit) ─
SELECT
    processing_status,
    COUNT(*) AS total_events,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct
FROM raw_happiness_events
GROUP BY processing_status
ORDER BY total_events DESC;

-- ─── KPI 6: Top 10 happiest countries (predicted avg) ─────────
SELECT
    dc.country_name,
    ROUND(AVG(fp.predicted_score)::NUMERIC, 4) AS avg_predicted_happiness
FROM fact_predictions fp
LEFT JOIN dim_country dc ON fp.country_id = dc.country_id
GROUP BY dc.country_name
HAVING COUNT(*) >= 1
ORDER BY avg_predicted_happiness DESC
LIMIT 10;
