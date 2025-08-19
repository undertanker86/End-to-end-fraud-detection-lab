-- ===============================================
-- PRACTICAL TRINO ANALYTICS QUERIES
-- Gold Layer Unified Analytics Examples  
-- ===============================================

-- 1. REAL-TIME vs BATCH FRAUD RATE COMPARISON
-- Compare fraud rates between batch processed and real-time data
WITH batch_metrics AS (
    SELECT 
        'batch' as data_type,
        hour,
        fraud_rate,
        total_transactions,
        created_at
    FROM minio.gold_batch.fraud_analytics
    WHERE created_at >= current_timestamp - interval '1' day
),
realtime_metrics AS (
    SELECT 
        'realtime' as data_type,
        EXTRACT(HOUR FROM data.time_stamp) as hour,
        CAST(SUM(CASE WHEN data.fraud_class = 1 THEN 1 ELSE 0 END) AS DOUBLE) / COUNT(*) as fraud_rate,
        COUNT(*) as total_transactions,
        MAX(data.processed_at) as created_at
    FROM minio.gold_stream.ml_features_realtime
    WHERE data.processed_at >= current_timestamp - interval '1' day
    GROUP BY EXTRACT(HOUR FROM data.time_stamp)
)
SELECT 
    hour,
    MAX(CASE WHEN data_type = 'batch' THEN fraud_rate END) as batch_fraud_rate,
    MAX(CASE WHEN data_type = 'realtime' THEN fraud_rate END) as realtime_fraud_rate,
    MAX(CASE WHEN data_type = 'batch' THEN total_transactions END) as batch_transactions,
    MAX(CASE WHEN data_type = 'realtime' THEN total_transactions END) as realtime_transactions
FROM (
    SELECT * FROM batch_metrics
    UNION ALL 
    SELECT * FROM realtime_metrics
)
GROUP BY hour
ORDER BY hour;

-- ===============================================

-- 2. ANOMALY DETECTION ACROSS DATA SOURCES
-- Identify transactions with high risk scores in real-time vs batch predictions
SELECT 
    'realtime' as source,
    data.id as transaction_id,
    data.amount,
    data.risk_score,
    data.is_anomaly,
    data.fraud_class as actual_fraud,
    data.processed_at
FROM minio.gold_stream.ml_features_realtime
WHERE data.risk_score > 0.8 OR data.is_anomaly = true
ORDER BY data.risk_score DESC
LIMIT 100;

-- ===============================================

-- 3. ML FEATURE QUALITY COMPARISON
-- Compare feature quality between batch and streaming pipelines
WITH batch_features AS (
    SELECT 
        'batch' as pipeline,
        COUNT(*) as total_records,
        AVG(dataquality_score) as avg_quality_score,
        AVG(outlier_ratio) as avg_outlier_ratio,
        MAX(created_at) as latest_update
    FROM minio.gold_batch.ml_features_current
),
stream_features AS (
    SELECT 
        'stream' as pipeline,
        COUNT(*) as total_records,
        AVG(CASE WHEN data.risk_score IS NOT NULL THEN 1.0 ELSE 0.5 END) as avg_quality_score,
        AVG(CASE WHEN data.is_anomaly THEN 1.0 ELSE 0.0 END) as avg_outlier_ratio,
        MAX(data.processed_at) as latest_update
    FROM minio.gold_stream.ml_features_realtime
)
SELECT * FROM batch_features
UNION ALL
SELECT * FROM stream_features;

-- ===============================================

-- 4. HOURLY TRANSACTION PATTERNS (UNIFIED VIEW)
-- Analyze transaction patterns across both data sources
WITH hourly_patterns AS (
    -- Batch data
    SELECT 
        hour,
        'batch' as source,
        total_transactions,
        fraud_count,
        avg_amount,
        fraud_rate,
        DATE(created_at) as analysis_date
    FROM minio.gold_batch.fraud_analytics
    
    UNION ALL
    
    -- Real-time data (aggregated)
    SELECT 
        EXTRACT(HOUR FROM data.time_stamp) as hour,
        'realtime' as source,
        COUNT(*) as total_transactions,
        SUM(data.fraud_class) as fraud_count,
        AVG(data.amount) as avg_amount,
        CAST(SUM(data.fraud_class) AS DOUBLE) / COUNT(*) as fraud_rate,
        DATE(data.processed_at) as analysis_date
    FROM minio.gold_stream.ml_features_realtime
    GROUP BY 
        EXTRACT(HOUR FROM data.time_stamp),
        DATE(data.processed_at)
)
SELECT 
    hour,
    analysis_date,
    SUM(CASE WHEN source = 'batch' THEN total_transactions ELSE 0 END) as batch_transactions,
    SUM(CASE WHEN source = 'realtime' THEN total_transactions ELSE 0 END) as realtime_transactions,
    AVG(CASE WHEN source = 'batch' THEN fraud_rate END) as batch_fraud_rate,
    AVG(CASE WHEN source = 'realtime' THEN fraud_rate END) as realtime_fraud_rate
FROM hourly_patterns
WHERE analysis_date >= current_date - interval '7' days
GROUP BY hour, analysis_date
ORDER BY analysis_date DESC, hour;

-- ===============================================

-- 5. TOP RISK FEATURES ANALYSIS
-- Identify which V* features contribute most to fraud detection
SELECT 
    'V1' as feature_name,
    AVG(CASE WHEN class = 1 THEN v1 ELSE NULL END) as avg_fraud_value,
    AVG(CASE WHEN class = 0 THEN v1 ELSE NULL END) as avg_normal_value,
    AVG(CASE WHEN class = 1 THEN v1 ELSE NULL END) - AVG(CASE WHEN class = 0 THEN v1 ELSE NULL END) as fraud_difference
FROM minio.gold_batch.ml_features_current

UNION ALL

SELECT 
    'V2' as feature_name,
    AVG(CASE WHEN class = 1 THEN v2 ELSE NULL END),
    AVG(CASE WHEN class = 0 THEN v2 ELSE NULL END),
    AVG(CASE WHEN class = 1 THEN v2 ELSE NULL END) - AVG(CASE WHEN class = 0 THEN v2 ELSE NULL END)
FROM minio.gold_batch.ml_features_current

-- Add more V* features as needed...

ORDER BY ABS(fraud_difference) DESC;

-- ===============================================

-- 6. DATA FRESHNESS MONITORING
-- Monitor how fresh the data is across different sources
SELECT 
    'fraud_analytics_batch' as table_name,
    COUNT(*) as record_count,
    MIN(created_at) as oldest_record,
    MAX(created_at) as newest_record,
    current_timestamp - MAX(created_at) as data_age
FROM minio.gold_batch.fraud_analytics

UNION ALL

SELECT 
    'ml_features_batch' as table_name,
    COUNT(*) as record_count,
    MIN(created_at) as oldest_record,
    MAX(created_at) as newest_record,
    current_timestamp - MAX(created_at) as data_age
FROM minio.gold_batch.ml_features_current

UNION ALL

SELECT 
    'ml_features_stream' as table_name,
    COUNT(*) as record_count,
    MIN(data.processed_at) as oldest_record,
    MAX(data.processed_at) as newest_record,
    current_timestamp - MAX(data.processed_at) as data_age
FROM minio.gold_stream.ml_features_realtime;

-- ===============================================

-- 7. ADVANCED: STREAMING vs BATCH PREDICTION ACCURACY
-- Compare predictions from both pipelines (when available)
WITH prediction_comparison AS (
    SELECT 
        b.class as batch_actual,
        CASE 
            WHEN b.outlier_ratio > 0.3 THEN 1 
            ELSE 0 
        END as batch_prediction,
        s.data.fraud_class as stream_actual,
        CASE 
            WHEN s.data.is_anomaly THEN 1 
            ELSE 0 
        END as stream_prediction,
        b.created_at as batch_time,
        s.data.processed_at as stream_time
    FROM minio.gold_batch.ml_features_current b
    FULL OUTER JOIN minio.gold_stream.ml_features_realtime s
        ON ABS(EXTRACT(EPOCH FROM b.created_at) - EXTRACT(EPOCH FROM s.data.processed_at)) < 3600 -- Within 1 hour
)
SELECT 
    COUNT(*) as total_comparisons,
    AVG(CASE WHEN batch_actual = batch_prediction THEN 1.0 ELSE 0.0 END) as batch_accuracy,
    AVG(CASE WHEN stream_actual = stream_prediction THEN 1.0 ELSE 0.0 END) as stream_accuracy,
    AVG(CASE WHEN batch_prediction = stream_prediction THEN 1.0 ELSE 0.0 END) as prediction_agreement
FROM prediction_comparison
WHERE batch_actual IS NOT NULL AND stream_actual IS NOT NULL;






