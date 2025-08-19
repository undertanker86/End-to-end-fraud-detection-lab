-- Create schemas for Gold layer data
-- Run these after Trino is up and running

-- Create schema for batch data (Parquet)
CREATE SCHEMA IF NOT EXISTS minio.gold_batch
WITH (location = 's3a://gold/batch/');

-- Create schema for streaming data (JSON) 
CREATE SCHEMA IF NOT EXISTS minio.gold_stream
WITH (location = 's3a://gold/stream/');

-- ==============================================
-- BATCH TABLES (Parquet)
-- ==============================================

-- Fraud Analytics (Hourly aggregations)
CREATE TABLE IF NOT EXISTS minio.gold_batch.fraud_analytics (
    timestamp TIMESTAMP,
    hour INTEGER,
    isweekend BOOLEAN,
    fraud_count BIGINT,
    total_transactions BIGINT,
    total_amount DOUBLE,
    avg_amount DOUBLE,
    median_amount DOUBLE,
    std_amount DOUBLE,
    avg_data_quality DOUBLE,
    fraud_rate DOUBLE,
    created_at TIMESTAMP,
    data_version VARCHAR
) WITH (
    external_location = 's3a://gold/fraud_analytics/',
    format = 'PARQUET'
);

-- ML Features (Transaction level)
CREATE TABLE IF NOT EXISTS minio.gold_batch.ml_features_current (
    v1 DOUBLE, v2 DOUBLE, v3 DOUBLE, v4 DOUBLE, v5 DOUBLE,
    v6 DOUBLE, v7 DOUBLE, v8 DOUBLE, v9 DOUBLE, v10 DOUBLE,
    v11 DOUBLE, v12 DOUBLE, v13 DOUBLE, v14 DOUBLE, v15 DOUBLE,
    v16 DOUBLE, v17 DOUBLE, v18 DOUBLE, v19 DOUBLE, v20 DOUBLE,
    v21 DOUBLE, v22 DOUBLE, v23 DOUBLE, v24 DOUBLE, v25 DOUBLE,
    v26 DOUBLE, v27 DOUBLE, v28 DOUBLE,
    amount_log DOUBLE,
    amount_percentile DOUBLE,
    hour INTEGER,
    dayofweek INTEGER,
    isweekend BOOLEAN,
    dataquality_score DOUBLE,
    class INTEGER,
    -- Outlier indicators
    v1_isoutlier INTEGER,
    v2_isoutlier INTEGER, 
    v3_isoutlier INTEGER,
    v4_isoutlier INTEGER,
    v5_isoutlier INTEGER,
    -- Engineered features
    v1_amount_interaction DOUBLE,
    v2_amount_interaction DOUBLE,
    hour_sin DOUBLE,
    hour_cos DOUBLE,
    amount_rolling_mean DOUBLE,
    amount_rolling_std DOUBLE,
    outlier_ratio DOUBLE,
    feature_version VARCHAR,
    created_at TIMESTAMP,
    data_source VARCHAR
) WITH (
    external_location = 's3a://gold/ml_features_current/',
    format = 'PARQUET'
);

-- ==============================================
-- STREAMING TABLES (JSON)
-- ==============================================

-- Real-time ML Features from Kafka/PyFlink
CREATE TABLE IF NOT EXISTS minio.gold_stream.ml_features_realtime (
    data ROW(
        id VARCHAR,
        time_stamp TIMESTAMP,
        v1 DOUBLE, v2 DOUBLE, v3 DOUBLE, v4 DOUBLE, v5 DOUBLE,
        v6 DOUBLE, v7 DOUBLE, v8 DOUBLE, v9 DOUBLE, v10 DOUBLE,
        v11 DOUBLE, v12 DOUBLE, v13 DOUBLE, v14 DOUBLE, v15 DOUBLE,
        v16 DOUBLE, v17 DOUBLE, v18 DOUBLE, v19 DOUBLE, v20 DOUBLE,
        v21 DOUBLE, v22 DOUBLE, v23 DOUBLE, v24 DOUBLE, v25 DOUBLE,
        v26 DOUBLE, v27 DOUBLE, v28 DOUBLE,
        amount DOUBLE,
        fraud_class INTEGER,
        processed_at TIMESTAMP,
        -- Engineered features
        amount_zscore DOUBLE,
        rolling_amount_mean DOUBLE,
        rolling_amount_std DOUBLE,
        risk_score DOUBLE,
        is_anomaly BOOLEAN,
        feature_version VARCHAR
    )
) WITH (
    external_location = 's3a://gold/ml_features_train_stream/',
    format = 'JSON'
);

-- ==============================================
-- VIEWS FOR UNIFIED ANALYTICS
-- ==============================================

-- Combined fraud analytics (batch + real-time)
CREATE VIEW minio.gold_batch.fraud_analytics_unified AS
SELECT 
    'batch' as source_type,
    timestamp,
    hour,
    fraud_count,
    total_transactions,
    fraud_rate,
    avg_amount,
    created_at
FROM minio.gold_batch.fraud_analytics

UNION ALL

SELECT 
    'realtime' as source_type,
    data.processed_at as timestamp,
    EXTRACT(HOUR FROM data.time_stamp) as hour,
    SUM(CASE WHEN data.fraud_class = 1 THEN 1 ELSE 0 END) as fraud_count,
    COUNT(*) as total_transactions,
    CAST(SUM(CASE WHEN data.fraud_class = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as fraud_rate,
    AVG(data.amount) as avg_amount,
    MAX(data.processed_at) as created_at
FROM minio.gold_stream.ml_features_realtime
GROUP BY 
    data.processed_at,
    EXTRACT(HOUR FROM data.time_stamp);






