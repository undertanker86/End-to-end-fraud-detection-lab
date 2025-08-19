## Trino + MinIO Gold Layer – Batch (Parquet) & Stream (JSON)

- End-to-end guide to query MinIO Gold layer from Trino for model training (batch Parquet + streaming JSON).


---

### 1) Prerequisites / Điều kiện
- English:
  - Docker + Docker Compose
  - Base stack up (includes MinIO): `docker compose up -d`
  - Trino stack up: `docker compose -f docker-compose-trino.yml up -d`
  - Ensure Trino and MinIO share a network: `fraud-detection-project_fraud-detection-network`


If needed:
```bash
# Attach containers to the MinIO network (chỉ khi khác network)
docker network connect fraud-detection-project_fraud-detection-network fraud-detection-project-trino-coordinator-1 || true
docker network connect fraud-detection-project_fraud-detection-network fraud-detection-project-trino-worker-1-1 || true
```

---

### 2) Trino MinIO Catalog Config / Config Catalog MinIO cho Trino
File: `trino/etc/catalog/minio.properties`
```properties
connector.name=hive

# S3/MinIO
fs.native-s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.aws-access-key=minioadmin
s3.aws-secret-key=minioadmin
s3.path-style-access=true

# Read nested hour folders for JSON stream
hive.recursive-directories=true

# Map Parquet columns by position (avoid case/name mismatch)
hive.parquet.use-column-names=false

# File-based metastore in MinIO
hive.metastore=file
hive.metastore.catalog.dir=s3://gold/
```
- Worker uses the same config in `trino/etc-worker/catalog/minio.properties`.

Restart Trino:
```bash
docker compose -f docker-compose-trino.yml restart
```

Notes:
- Use modern keys `s3.*` (not `hive.s3.*`). Remove unsupported ones like `s3.ssl.enabled`.
- Add `s3.region` to fix AWS region errors.

---

### 3) Connect via DBeaver
- English:
  - Driver: Trino
  - Host: `localhost`, Port: `8090`
  - User: any (no auth configured), SSL: off

Quick checks:
```sql
SHOW CATALOGS;                      -- expect: memory, minio, system, tpch
SHOW SCHEMAS IN minio;              -- expect: information_schema, gold_ml, gold_streaming, ...
```

---

### 4) Schemas & Tables
Create schemas:
```sql
CREATE SCHEMA IF NOT EXISTS minio.gold_ml;
CREATE SCHEMA IF NOT EXISTS minio.gold_analytics;
CREATE SCHEMA IF NOT EXISTS minio.gold_streaming;
```

Batch Parquet – define once then reuse:
```sql
-- Current dataset (align types with Parquet files; outlier flags are INT)
DROP TABLE IF EXISTS minio.gold_ml.ml_features_current;
CREATE TABLE minio.gold_ml.ml_features_current (
  time DOUBLE,
  v1 DOUBLE, v2 DOUBLE, v3 DOUBLE, v4 DOUBLE, v5 DOUBLE,
  v6 DOUBLE, v7 DOUBLE, v8 DOUBLE, v9 DOUBLE, v10 DOUBLE,
  v11 DOUBLE, v12 DOUBLE, v13 DOUBLE, v14 DOUBLE, v15 DOUBLE,
  v16 DOUBLE, v17 DOUBLE, v18 DOUBLE, v19 DOUBLE, v20 DOUBLE,
  v21 DOUBLE, v22 DOUBLE, v23 DOUBLE, v24 DOUBLE, v25 DOUBLE,
  v26 DOUBLE, v27 DOUBLE, v28 DOUBLE,
  amount DOUBLE, class BIGINT,
  v1_isoutlier INTEGER, v2_isoutlier INTEGER, v3_isoutlier INTEGER, v4_isoutlier INTEGER,
  v5_isoutlier INTEGER, v6_isoutlier INTEGER, v7_isoutlier INTEGER, v8_isoutlier INTEGER,
  v9_isoutlier INTEGER, v10_isoutlier INTEGER, v11_isoutlier INTEGER, v12_isoutlier INTEGER,
  v13_isoutlier INTEGER, v14_isoutlier INTEGER, v15_isoutlier INTEGER, v16_isoutlier INTEGER,
  v17_isoutlier INTEGER, v18_isoutlier INTEGER, v19_isoutlier INTEGER, v20_isoutlier INTEGER,
  v21_isoutlier INTEGER, v22_isoutlier INTEGER, v23_isoutlier INTEGER, v24_isoutlier INTEGER,
  v25_isoutlier INTEGER, v26_isoutlier INTEGER, v27_isoutlier INTEGER, v28_isoutlier INTEGER,
  amount_isoutlier INTEGER
) WITH (external_location='s3://gold/ml_features_current.parquet/', format='PARQUET');

-- Train/Test reuse schema (LIKE) with different locations
DROP TABLE IF EXISTS minio.gold_ml.ml_features_train;
CREATE TABLE minio.gold_ml.ml_features_train
(LIKE minio.gold_ml.ml_features_current EXCLUDING PROPERTIES)
WITH (external_location='s3://gold/ml_features_train.parquet/', format='PARQUET');

DROP TABLE IF EXISTS minio.gold_ml.ml_features_test;
CREATE TABLE minio.gold_ml.ml_features_test
(LIKE minio.gold_ml.ml_features_current EXCLUDING PROPERTIES)
WITH (external_location='s3://gold/ml_features_test.parquet/', format='PARQUET');
```

Streaming JSON (hour folders):
```sql
-- Raw JSON table (recursive directories already enabled in catalog)
CREATE TABLE IF NOT EXISTS minio.gold_streaming.ml_features_stream_raw (
  json_data VARCHAR
) WITH (external_location='s3://gold/ml_features_train_stream/', format='TEXTFILE');

-- Optional parsed view (tuỳ chọn)
CREATE OR REPLACE VIEW minio.gold_streaming.ml_features_stream_parsed AS
SELECT
  CAST(json_extract_scalar(json_data, '$.time') AS DOUBLE)   AS time,
  CAST(json_extract_scalar(json_data, '$.amount') AS DOUBLE) AS amount,
  CAST(json_extract_scalar(json_data, '$.fraud_class') AS BIGINT) AS class,
  CAST(json_extract_scalar(json_data, '$.risk_score') AS DOUBLE)  AS risk_score,
  CAST(json_extract_scalar(json_data, '$.v1') AS DOUBLE) AS v1,
  CAST(json_extract_scalar(json_data, '$.v2') AS DOUBLE) AS v2,
  CAST(json_extract_scalar(json_data, '$.v3') AS DOUBLE) AS v3,
  regexp_extract("$path", '.*/(\\d{4}-\\d{2}-\\d{2}--\\d{2})/.*', 1) AS hour_bucket
FROM minio.gold_streaming.ml_features_stream_raw;
```

---

### 5) Query Examples
Batch (Parquet):
```sql
-- Counts and schema
SELECT COUNT(*) FROM minio.gold_ml.ml_features_current;
SELECT class, COUNT(*) FROM minio.gold_ml.ml_features_train GROUP BY class ORDER BY class;

-- Sample feature rows for training
SELECT
  v1,v2,v3,v4,v5,v6,v7,v8,v9,v10,
  v11,v12,v13,v14,v15,v16,v17,v18,v19,v20,
  v21,v22,v23,v24,v25,v26,v27,v28,
  amount, class
FROM minio.gold_ml.ml_features_train
WHERE amount IS NOT NULL
LIMIT 1000;

-- Use outlier flags as booleans
SELECT (v1_isoutlier=1) AS v1_isoutlier_bool, amount, class
FROM minio.gold_ml.ml_features_current
LIMIT 10;
```

Streaming (JSON):
```sql
SELECT COUNT(*) FROM minio.gold_streaming.ml_features_stream_raw;
SELECT * FROM minio.gold_streaming.ml_features_stream_parsed ORDER BY hour_bucket DESC LIMIT 10;
```

---

### 6) Troubleshooting
  - Error: property 'hive.s3.*' was not used → Switch to modern `s3.*` keys, remove `s3.ssl.enabled`.
  - Error: Unable to load region → Add `s3.region=us-east-1`.
  - Session property not found (`Catalog 'hive' not found`) → If using `SET SESSION`, prefix with the catalog name (e.g., `minio`), but we already set recursive dirs in catalog.
  - Zero rows from streaming JSON → Ensure `hive.recursive-directories=true` in catalog and correct external path.
  - Parquet boolean vs int32 mismatch → Define outlier columns as `INTEGER` (or cast in queries).
  - Amount nulls / column mismatch → Use `hive.parquet.use-column-names=false` to map by position.
  - Network errors → Ensure Trino and MinIO share the same Docker network.

---

### 7) Handy commands
```bash
# Trino HTTP info
curl -s http://localhost:8090/v1/info | jq .

# Trino CLI inside container
docker exec -it fraud-detection-project-trino-coordinator-1 trino --server localhost:8080

# Run helper (creates base schemas)
bash ./query_trino.sh
```

---

### 8) Security & Notes
- Dev credentials (`minioadmin`) are for local only. For prod, use secrets/env and HTTPS endpoint.


---

### 9) Outcome
- You can query batch Parquet datasets and streaming JSON in MinIO gold from Trino for analytics and training.
