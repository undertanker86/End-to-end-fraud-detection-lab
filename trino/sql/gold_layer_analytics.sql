-- Gold Layer Analytics Queries for Real MinIO Data
-- Query actual fraud detection data from Gold layer

-- ==============================================
-- DATA EXPLORATION QUERIES
-- ==============================================

-- 1. Check data availability across datasets
SELECT 
    'ml_features_current' as table_name,
    COUNT(*) as record_count
FROM minio.gold_ml.ml_features_current

UNION ALL

SELECT 
    'ml_features_train' as table_name,
    COUNT(*) as record_count  
FROM minio.gold_ml.ml_features_train

UNION ALL

SELECT 
    'ml_features_test' as table_name,
    COUNT(*) as record_count
FROM minio.gold_ml.ml_features_test;

-- 2. Fraud analysis across all datasets
SELECT 
    dataset_type,
    total_transactions,
    fraud_transactions,
    ROUND(fraud_rate * 100, 4) as fraud_rate_percent,
    ROUND(avg_amount, 2) as avg_amount,
    ROUND(stddev_amount, 2) as stddev_amount,
    ROUND(avg_outlier_features_per_transaction, 2) as avg_outlier_features
FROM minio.gold_ml.fraud_summary
ORDER BY fraud_rate DESC;

-- ==============================================
-- FRAUD DETECTION ANALYTICS
-- ==============================================

-- 3. Transaction amount distribution by fraud class
SELECT 
    Class as fraud_class,
    COUNT(*) as transaction_count,
    ROUND(AVG(Amount), 2) as avg_amount,
    ROUND(STDDEV(Amount), 2) as stddev_amount,
    ROUND(MIN(Amount), 2) as min_amount,
    ROUND(MAX(Amount), 2) as max_amount,
    ROUND(PERCENTILE(Amount, 0.5), 2) as median_amount,
    ROUND(PERCENTILE(Amount, 0.95), 2) as p95_amount
FROM minio.gold_ml.ml_features_current
GROUP BY Class
ORDER BY Class;

-- 4. Time-based fraud patterns (using Time feature)
SELECT 
    FLOOR(Time / 3600) as hour_bucket,
    COUNT(*) as total_transactions,
    SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CAST(Class AS DOUBLE)) * 100, 4) as fraud_rate_percent,
    ROUND(AVG(Amount), 2) as avg_amount
FROM minio.gold_ml.ml_features_current
GROUP BY FLOOR(Time / 3600)
ORDER BY hour_bucket
LIMIT 24;

-- 5. Outlier analysis - which features are most indicative of fraud
WITH outlier_analysis AS (
    SELECT 
        Class,
        CAST(V1_IsOutlier AS INTEGER) as V1_outlier,
        CAST(V2_IsOutlier AS INTEGER) as V2_outlier,
        CAST(V3_IsOutlier AS INTEGER) as V3_outlier,
        CAST(V4_IsOutlier AS INTEGER) as V4_outlier,
        CAST(V5_IsOutlier AS INTEGER) as V5_outlier,
        CAST(Amount_IsOutlier AS INTEGER) as Amount_outlier
    FROM minio.gold_ml.ml_features_current
)
SELECT 
    'V1_IsOutlier' as feature_name,
    ROUND(AVG(CASE WHEN Class = 1 THEN V1_outlier ELSE NULL END) * 100, 2) as fraud_outlier_rate,
    ROUND(AVG(CASE WHEN Class = 0 THEN V1_outlier ELSE NULL END) * 100, 2) as normal_outlier_rate
FROM outlier_analysis

UNION ALL

SELECT 
    'V2_IsOutlier' as feature_name,
    ROUND(AVG(CASE WHEN Class = 1 THEN V2_outlier ELSE NULL END) * 100, 2),
    ROUND(AVG(CASE WHEN Class = 0 THEN V2_outlier ELSE NULL END) * 100, 2)
FROM outlier_analysis

UNION ALL

SELECT 
    'Amount_IsOutlier' as feature_name,
    ROUND(AVG(CASE WHEN Class = 1 THEN Amount_outlier ELSE NULL END) * 100, 2),
    ROUND(AVG(CASE WHEN Class = 0 THEN Amount_outlier ELSE NULL END) * 100, 2)
FROM outlier_analysis;

-- ==============================================
-- FEATURE ENGINEERING ANALYTICS
-- ==============================================

-- 6. Principal component analysis - check V1-V28 feature distributions
SELECT 
    'V1' as feature_name,
    ROUND(AVG(V1), 6) as avg_value,
    ROUND(STDDEV(V1), 6) as stddev_value,
    ROUND(MIN(V1), 6) as min_value,
    ROUND(MAX(V1), 6) as max_value
FROM minio.gold_ml.ml_features_current

UNION ALL

SELECT 
    'V2' as feature_name,
    ROUND(AVG(V2), 6), ROUND(STDDEV(V2), 6), 
    ROUND(MIN(V2), 6), ROUND(MAX(V2), 6)
FROM minio.gold_ml.ml_features_current

UNION ALL

SELECT 
    'Amount' as feature_name,
    ROUND(AVG(Amount), 2), ROUND(STDDEV(Amount), 2),
    ROUND(MIN(Amount), 2), ROUND(MAX(Amount), 2)
FROM minio.gold_ml.ml_features_current;

-- 7. Cross-dataset comparison for data quality
WITH dataset_stats AS (
    SELECT 
        'current' as dataset,
        COUNT(*) as record_count,
        AVG(CAST(Class AS DOUBLE)) as fraud_rate,
        AVG(Amount) as avg_amount,
        COUNT(CASE WHEN V1_IsOutlier THEN 1 END) as V1_outliers
    FROM minio.gold_ml.ml_features_current
    
    UNION ALL
    
    SELECT 
        'train' as dataset,
        COUNT(*) as record_count,
        AVG(CAST(Class AS DOUBLE)) as fraud_rate,
        AVG(Amount) as avg_amount,
        COUNT(CASE WHEN V1_IsOutlier THEN 1 END) as V1_outliers
    FROM minio.gold_ml.ml_features_train
    
    UNION ALL
    
    SELECT 
        'test' as dataset,
        COUNT(*) as record_count,
        AVG(CAST(Class AS DOUBLE)) as fraud_rate,
        AVG(Amount) as avg_amount,
        COUNT(CASE WHEN V1_IsOutlier THEN 1 END) as V1_outliers
    FROM minio.gold_ml.ml_features_test
)
SELECT 
    dataset,
    record_count,
    ROUND(fraud_rate * 100, 4) as fraud_rate_percent,
    ROUND(avg_amount, 2) as avg_amount,
    V1_outliers,
    ROUND(CAST(V1_outliers AS DOUBLE) / record_count * 100, 2) as outlier_percentage
FROM dataset_stats
ORDER BY record_count DESC;

-- ==============================================
-- STREAMING DATA ANALYSIS
-- ==============================================

-- 8. Check streaming data (JSON files)
-- Note: This will show raw JSON content for inspection
SELECT 
    json_data
FROM minio.gold_streaming.ml_features_stream_raw 
LIMIT 5;

-- ==============================================
-- ML MODEL EVALUATION QUERIES  
-- ==============================================

-- 9. Feature correlation with fraud (simplified)
-- High-value transactions analysis
SELECT 
    CASE 
        WHEN Amount < 10 THEN '< $10'
        WHEN Amount < 50 THEN '$10-$50' 
        WHEN Amount < 100 THEN '$50-$100'
        WHEN Amount < 500 THEN '$100-$500'
        ELSE '> $500'
    END as amount_range,
    COUNT(*) as transaction_count,
    SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) as fraud_count,
    ROUND(AVG(CAST(Class AS DOUBLE)) * 100, 4) as fraud_rate_percent
FROM minio.gold_ml.ml_features_current
GROUP BY 
    CASE 
        WHEN Amount < 10 THEN '< $10'
        WHEN Amount < 50 THEN '$10-$50'
        WHEN Amount < 100 THEN '$50-$100' 
        WHEN Amount < 500 THEN '$100-$500'
        ELSE '> $500'
    END
ORDER BY fraud_rate_percent DESC;

-- 10. Sample transactions for manual inspection
SELECT 
    Time,
    ROUND(V1, 4) as V1, ROUND(V2, 4) as V2, ROUND(V3, 4) as V3,
    ROUND(Amount, 2) as Amount,
    Class as fraud_class,
    V1_IsOutlier, V2_IsOutlier, Amount_IsOutlier,
    CASE WHEN Class = 1 THEN 'FRAUD' ELSE 'NORMAL' END as transaction_type
FROM minio.gold_ml.ml_features_current
WHERE Class = 1  -- Focus on fraud cases
ORDER BY Amount DESC
LIMIT 20;


