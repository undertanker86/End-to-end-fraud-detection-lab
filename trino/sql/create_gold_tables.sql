-- Create tables for actual Gold Layer data in MinIO
-- Based on real data structure analysis

-- ==============================================
-- CREATE SCHEMAS
-- ==============================================

-- Create schema for ML features (batch data)
CREATE SCHEMA IF NOT EXISTS minio.gold_ml;

-- Create schema for analytics data  
CREATE SCHEMA IF NOT EXISTS minio.gold_analytics;

-- Create schema for streaming data
CREATE SCHEMA IF NOT EXISTS minio.gold_streaming;

-- ==============================================
-- BATCH ANALYTICS TABLES (Parquet)
-- ==============================================

-- ML Features Current Dataset (7.29 MB)
DROP TABLE IF EXISTS minio.gold_ml.ml_features_current;
CREATE TABLE minio.gold_ml.ml_features_current (
    Time DOUBLE,
    V1 DOUBLE, V2 DOUBLE, V3 DOUBLE, V4 DOUBLE, V5 DOUBLE,
    V6 DOUBLE, V7 DOUBLE, V8 DOUBLE, V9 DOUBLE, V10 DOUBLE,
    V11 DOUBLE, V12 DOUBLE, V13 DOUBLE, V14 DOUBLE, V15 DOUBLE,
    V16 DOUBLE, V17 DOUBLE, V18 DOUBLE, V19 DOUBLE, V20 DOUBLE,
    V21 DOUBLE, V22 DOUBLE, V23 DOUBLE, V24 DOUBLE, V25 DOUBLE,
    V26 DOUBLE, V27 DOUBLE, V28 DOUBLE, Amount DOUBLE, Class BIGINT,
    -- Outlier detection features
    V1_IsOutlier BOOLEAN, V2_IsOutlier BOOLEAN, V3_IsOutlier BOOLEAN,
    V4_IsOutlier BOOLEAN, V5_IsOutlier BOOLEAN, V6_IsOutlier BOOLEAN,
    V7_IsOutlier BOOLEAN, V8_IsOutlier BOOLEAN, V9_IsOutlier BOOLEAN,
    V10_IsOutlier BOOLEAN, V11_IsOutlier BOOLEAN, V12_IsOutlier BOOLEAN,
    V13_IsOutlier BOOLEAN, V14_IsOutlier BOOLEAN, V15_IsOutlier BOOLEAN,
    V16_IsOutlier BOOLEAN, V17_IsOutlier BOOLEAN, V18_IsOutlier BOOLEAN,
    V19_IsOutlier BOOLEAN, V20_IsOutlier BOOLEAN, V21_IsOutlier BOOLEAN,
    V22_IsOutlier BOOLEAN, V23_IsOutlier BOOLEAN, V24_IsOutlier BOOLEAN,
    V25_IsOutlier BOOLEAN, V26_IsOutlier BOOLEAN, V27_IsOutlier BOOLEAN,
    V28_IsOutlier BOOLEAN, Amount_IsOutlier BOOLEAN
) WITH (
    external_location = 's3://gold/ml_features_current.parquet/',
    format = 'PARQUET'
);

-- ML Features Training Dataset (5.87 MB)
DROP TABLE IF EXISTS minio.gold_ml.ml_features_train;
CREATE TABLE minio.gold_ml.ml_features_train (
    Time DOUBLE,
    V1 DOUBLE, V2 DOUBLE, V3 DOUBLE, V4 DOUBLE, V5 DOUBLE,
    V6 DOUBLE, V7 DOUBLE, V8 DOUBLE, V9 DOUBLE, V10 DOUBLE,
    V11 DOUBLE, V12 DOUBLE, V13 DOUBLE, V14 DOUBLE, V15 DOUBLE,
    V16 DOUBLE, V17 DOUBLE, V18 DOUBLE, V19 DOUBLE, V20 DOUBLE,
    V21 DOUBLE, V22 DOUBLE, V23 DOUBLE, V24 DOUBLE, V25 DOUBLE,
    V26 DOUBLE, V27 DOUBLE, V28 DOUBLE, Amount DOUBLE, Class BIGINT,
    -- Outlier detection features  
    V1_IsOutlier BOOLEAN, V2_IsOutlier BOOLEAN, V3_IsOutlier BOOLEAN,
    V4_IsOutlier BOOLEAN, V5_IsOutlier BOOLEAN, V6_IsOutlier BOOLEAN,
    V7_IsOutlier BOOLEAN, V8_IsOutlier BOOLEAN, V9_IsOutlier BOOLEAN,
    V10_IsOutlier BOOLEAN, V11_IsOutlier BOOLEAN, V12_IsOutlier BOOLEAN,
    V13_IsOutlier BOOLEAN, V14_IsOutlier BOOLEAN, V15_IsOutlier BOOLEAN,
    V16_IsOutlier BOOLEAN, V17_IsOutlier BOOLEAN, V18_IsOutlier BOOLEAN,
    V19_IsOutlier BOOLEAN, V20_IsOutlier BOOLEAN, V21_IsOutlier BOOLEAN,
    V22_IsOutlier BOOLEAN, V23_IsOutlier BOOLEAN, V24_IsOutlier BOOLEAN,
    V25_IsOutlier BOOLEAN, V26_IsOutlier BOOLEAN, V27_IsOutlier BOOLEAN,
    V28_IsOutlier BOOLEAN, Amount_IsOutlier BOOLEAN
) WITH (
    external_location = 's3://gold/ml_features_train.parquet/',
    format = 'PARQUET'
);

-- ML Features Test Dataset (1.47 MB)
DROP TABLE IF EXISTS minio.gold_ml.ml_features_test;
CREATE TABLE minio.gold_ml.ml_features_test (
    Time DOUBLE,
    V1 DOUBLE, V2 DOUBLE, V3 DOUBLE, V4 DOUBLE, V5 DOUBLE,
    V6 DOUBLE, V7 DOUBLE, V8 DOUBLE, V9 DOUBLE, V10 DOUBLE,
    V11 DOUBLE, V12 DOUBLE, V13 DOUBLE, V14 DOUBLE, V15 DOUBLE,
    V16 DOUBLE, V17 DOUBLE, V18 DOUBLE, V19 DOUBLE, V20 DOUBLE,
    V21 DOUBLE, V22 DOUBLE, V23 DOUBLE, V24 DOUBLE, V25 DOUBLE,
    V26 DOUBLE, V27 DOUBLE, V28 DOUBLE, Amount DOUBLE, Class BIGINT,
    -- Outlier detection features
    V1_IsOutlier BOOLEAN, V2_IsOutlier BOOLEAN, V3_IsOutlier BOOLEAN,
    V4_IsOutlier BOOLEAN, V5_IsOutlier BOOLEAN, V6_IsOutlier BOOLEAN,
    V7_IsOutlier BOOLEAN, V8_IsOutlier BOOLEAN, V9_IsOutlier BOOLEAN,
    V10_IsOutlier BOOLEAN, V11_IsOutlier BOOLEAN, V12_IsOutlier BOOLEAN,
    V13_IsOutlier BOOLEAN, V14_IsOutlier BOOLEAN, V15_IsOutlier BOOLEAN,
    V16_IsOutlier BOOLEAN, V17_IsOutlier BOOLEAN, V18_IsOutlier BOOLEAN,
    V19_IsOutlier BOOLEAN, V20_IsOutlier BOOLEAN, V21_IsOutlier BOOLEAN,
    V22_IsOutlier BOOLEAN, V23_IsOutlier BOOLEAN, V24_IsOutlier BOOLEAN,
    V25_IsOutlier BOOLEAN, V26_IsOutlier BOOLEAN, V27_IsOutlier BOOLEAN,
    V28_IsOutlier BOOLEAN, Amount_IsOutlier BOOLEAN
) WITH (
    external_location = 's3://gold/ml_features_test.parquet/',
    format = 'PARQUET'
);

-- Fraud Analytics Hourly (currently minimal data)
DROP TABLE IF EXISTS minio.gold_analytics.fraud_analytics_hourly;
CREATE TABLE minio.gold_analytics.fraud_analytics_hourly (
    hour_window TIMESTAMP,
    total_transactions BIGINT,
    fraud_transactions BIGINT,
    fraud_rate DOUBLE,
    total_amount DOUBLE,
    avg_amount DOUBLE,
    max_amount DOUBLE,
    processing_timestamp TIMESTAMP
) WITH (
    external_location = 's3://gold/fraud_analytics_hourly.parquet/',
    format = 'PARQUET'
);

-- ==============================================
-- STREAMING DATA TABLES (JSON)
-- ==============================================

-- ML Features Streaming Data
-- Note: JSON data in MinIO requires flexible schema
DROP TABLE IF EXISTS minio.gold_streaming.ml_features_stream_raw;
CREATE TABLE minio.gold_streaming.ml_features_stream_raw (
    json_data VARCHAR -- Store full JSON as text for flexible parsing
) WITH (
    external_location = 's3://gold/ml_features_train_stream/',
    format = 'TEXTFILE' -- Use TEXTFILE for raw JSON
);

-- ==============================================
-- ANALYTICAL VIEWS
-- ==============================================

-- Combined view of all ML features for analysis
CREATE VIEW minio.gold_ml.ml_features_combined AS
SELECT 
    'current' as dataset_type,
    Time, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10,
    V11, V12, V13, V14, V15, V16, V17, V18, V19, V20,
    V21, V22, V23, V24, V25, V26, V27, V28, 
    Amount, Class,
    -- Count total outliers per transaction
    (CAST(V1_IsOutlier AS INTEGER) + CAST(V2_IsOutlier AS INTEGER) + 
     CAST(V3_IsOutlier AS INTEGER) + CAST(V4_IsOutlier AS INTEGER) + 
     CAST(V5_IsOutlier AS INTEGER)) as outlier_count_sample
FROM minio.gold_ml.ml_features_current

UNION ALL

SELECT 
    'train' as dataset_type,
    Time, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10,
    V11, V12, V13, V14, V15, V16, V17, V18, V19, V20,
    V21, V22, V23, V24, V25, V26, V27, V28,
    Amount, Class,
    (CAST(V1_IsOutlier AS INTEGER) + CAST(V2_IsOutlier AS INTEGER) + 
     CAST(V3_IsOutlier AS INTEGER) + CAST(V4_IsOutlier AS INTEGER) + 
     CAST(V5_IsOutlier AS INTEGER)) as outlier_count_sample
FROM minio.gold_ml.ml_features_train

UNION ALL

SELECT 
    'test' as dataset_type,
    Time, V1, V2, V3, V4, V5, V6, V7, V8, V9, V10,
    V11, V12, V13, V14, V15, V16, V17, V18, V19, V20,
    V21, V22, V23, V24, V25, V26, V27, V28,
    Amount, Class,
    (CAST(V1_IsOutlier AS INTEGER) + CAST(V2_IsOutlier AS INTEGER) + 
     CAST(V3_IsOutlier AS INTEGER) + CAST(V4_IsOutlier AS INTEGER) + 
     CAST(V5_IsOutlier AS INTEGER)) as outlier_count_sample
FROM minio.gold_ml.ml_features_test;

-- Fraud analysis summary view
CREATE VIEW minio.gold_ml.fraud_summary AS
SELECT 
    dataset_type,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) as fraud_transactions,
    AVG(CAST(Class AS DOUBLE)) as fraud_rate,
    AVG(Amount) as avg_amount,
    STDDEV(Amount) as stddev_amount,
    MIN(Amount) as min_amount,
    MAX(Amount) as max_amount,
    AVG(outlier_count_sample) as avg_outlier_features_per_transaction
FROM minio.gold_ml.ml_features_combined
GROUP BY dataset_type;

