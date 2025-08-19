# 🚀 Fraud Detection ETL Pipeline with Apache Airflow

## 📋 **Table of Contents**
- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Pipeline Components](#pipeline-components)
- [Usage Instructions](#usage-instructions)
- [Monitoring & Troubleshooting](#monitoring--troubleshooting)
- [Data Flow](#data-flow)
- [Advanced Configuration](#advanced-configuration)

---

## 🎯 **Overview**

This project implements a **production-ready batch ETL pipeline** for fraud detection using Apache Airflow, featuring:

- **Bronze → Silver → Gold** data lakehouse architecture
- **Batch processing** with Apache Spark and PySpark
- **Data storage** in MinIO object storage
- **Data quality** validation with Great Expectations
- **ML feature engineering** for fraud detection models
- **Automated orchestration** with Apache Airflow

---

## 🏗️ **Architecture**

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Raw Data      │    │   MinIO         │    │   ML Features   │
│   (CSV Files)   │───▶│   Object Store  │───▶│   (Parquet)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Bronze Layer  │    │   Silver Layer  │    │   Gold Layer    │
│   (Raw/Cleaned) │───▶│   (Enriched)    │───▶│   (ML Features) │
└─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Airflow DAGs  │    │   Spark Jobs    │    │   ML Analytics  │
│   (Orchestration)│   │   (Processing)  │    │   (Insights)    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

---

## ✅ **Prerequisites**

### **System Requirements**
- Docker & Docker Compose
- 8GB+ RAM available
- 20GB+ disk space
- Linux/macOS/Windows with Docker support

### **Required Services**
- **Core Stack**: MinIO (object storage)
- **Batch Stack**: Airflow, PostgreSQL, Redis
- **Processing**: Apache Spark (PySpark)

---

## 🚀 **Quick Start**

### **1. Clone & Setup**
```bash
git clone <your-repo>
cd Fraud-Detection-Project
chmod +x start_airflow_pipeline.sh
```

### **2. Start All Services (Recommended)**
```bash
# One-command startup
./start_airflow_pipeline.sh
```

### **3. Manual Startup (Alternative)**
```bash
# Start core infrastructure (MinIO)
docker compose up -d

# Start Airflow stack
docker compose -f docker-compose-airflow.yml up -d
```

---

## 🔧 **Pipeline Components**

### **Core Infrastructure**
```
📁 Project Structure:
├── airflow/                          # Airflow configuration
│   ├── dags/                        # ETL pipeline DAGs
│   │   ├── bronze_to_silver_etl.py  # Bronze → Silver pipeline
│   │   └── silver_to_gold_etl.py    # Silver → Gold pipeline
│   ├── logs/                        # Airflow execution logs
│   ├── plugins/                     # Custom Airflow plugins
│   └── requirements.txt             # Python dependencies
├── configs/                         # Configuration templates
├── docker-compose-airflow.yml       # Airflow service orchestration
└── start_airflow_pipeline.sh        # Easy startup script
```

### **Data Layers**
```
🗂️ Bronze Layer (Raw Data):
├── Raw CSV files                   # Source transaction data
├── Initial validation              # Basic data quality checks
└── Schema enforcement              # Data type validation

🔧 Silver Layer (Cleaned Data):
├── Data cleaning                   # Handle missing values
├── Feature engineering             # Basic transformations
├── Outlier detection               # Statistical analysis
└── Quality scoring                # Data quality metrics

🏆 Gold Layer (ML Features):
├── ML features                    # Engineered features for ML
├── Training datasets              # Train/test splits
├── Feature statistics             # Statistical summaries
└── Parquet files                 # Optimized data format
```

---

## 📊 **Usage Instructions**

### **1. Data Pipeline Execution**

#### **Upload Raw Data to Bronze**
```bash
# Upload source data to bronze layer
python3 upload_to_bronze.py
```

#### **Execute ETL Pipelines**
```bash
# Access Airflow UI: http://localhost:8080
# Default credentials: airflow/airflow

# Trigger DAGs manually
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags trigger bronze_to_silver_etl

docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags trigger silver_to_gold_etl
```

#### **Monitor DAG Execution**
```bash
# Check DAG runs
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags list-runs -d bronze_to_silver_etl

# List all DAGs
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags list
```

---

## 📈 **Monitoring & Troubleshooting**

### **Service Status Checks**
```bash
# Check Airflow services
docker compose -f docker-compose-airflow.yml ps

# Check MinIO service
docker compose ps minio
```

### **Common Issues & Solutions**

#### **1. DAG Management**
```bash
# Unpause paused DAGs
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags unpause silver_to_gold_etl

# Clear failed tasks
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow tasks clear bronze_to_silver_etl --task-regex "upload_to_silver" --yes

# Check DAG status
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags state bronze_to_silver_etl
```

#### **2. Dependency Issues**
```bash
# Install required Python packages
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 bash

# Inside container:
pip install --no-cache-dir \
  pyspark==3.5.1 \
  numpy==1.26.4 \
  pandas==2.1.4 \
  pyarrow==14.0.2 \
  minio==7.1.15 \
  great-expectations==0.17.23
```

#### **3. Spark Configuration Issues**
```bash
# Check Spark session in Airflow logs
docker compose -f docker-compose-airflow.yml logs airflow-worker-1 | grep -i spark

# Verify JAR files are available
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 ls -la /opt/airflow/jars/
```

### **Log Analysis**
```bash
# Airflow scheduler logs
docker compose -f docker-compose-airflow.yml logs airflow-scheduler-1 | grep ERROR

# Airflow worker logs
docker compose -f docker-compose-airflow.yml logs airflow-worker-1 | grep ERROR

# Check specific task logs
docker compose -f docker-compose-airflow.yml logs airflow-worker-1 | grep "Task failed"
```

---

## 🔄 **Data Flow**

### **Batch Processing Flow**
```
1. Raw Data (CSV) → Bronze Layer
   ├── Data validation with Great Expectations
   ├── Schema enforcement
   └── Quality scoring

2. Bronze → Silver Layer
   ├── Data cleaning (missing values, outliers)
   ├── Feature engineering (amount transformations)
   ├── Statistical analysis
   └── Quality metrics calculation

3. Silver → Gold Layer
   ├── ML feature engineering
   ├── Dataset splitting (train/test)
   ├── Feature statistics
   └── Parquet file generation
```

### **Data Quality Pipeline**
```
1. Great Expectations Validation
   ├── Column value checks
   ├── Data type validation
   ├── Statistical validation
   └── Custom business rules

2. Data Profiling
   ├── Missing value analysis
   ├── Outlier detection
   ├── Distribution analysis
   └── Correlation analysis

3. Quality Scoring
   ├── Completeness score
   ├── Accuracy score
   ├── Consistency score
   └── Overall quality metric
```

---

## ⚙️ **Advanced Configuration**

### **Airflow Configuration**
```yaml
# Key Airflow settings in docker-compose-airflow.yml
AIRFLOW__CORE__EXECUTOR: CeleryExecutor
AIRFLOW__CELERY__WORKER_CONCURRENCY: 1
AIRFLOW__SCHEDULER__PARSING_PROCESSES: 2
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
```

### **Spark Configuration**
```python
# Spark session configuration in DAGs
spark = (
    SparkSession.builder
    .appName('fraud_detection_etl')
    .master('local[1]')
    .config('spark.driver.memory', '1g')
    .config('spark.executor.memory', '1g')
    .config('spark.sql.parquet.compression.codec', 'uncompressed')
    .config('spark.hadoop.fs.s3a.endpoint', 'http://minio:9000')
    .config('spark.hadoop.fs.s3a.access.key', 'minioadmin')
    .config('spark.hadoop.fs.s3a.secret.key', 'minioadmin')
    .getOrCreate()
)
```

### **MinIO Configuration**
```properties
# MinIO connection settings
AWS_ENDPOINT_URL: http://minio:9000
AWS_ACCESS_KEY_ID: minioadmin
AWS_SECRET_ACCESS_KEY: minioadmin
```

---

## 🎯 **Access Points**

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Airflow UI** | http://localhost:8080 | airflow/airflow | Pipeline orchestration |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin | Object storage management |

---

## 🛑 **Shutdown & Cleanup**

### **Stop All Services**
```bash
# Stop Airflow stack
docker compose -f docker-compose-airflow.yml down

# Stop MinIO
docker compose down

# Remove volumes (optional - will delete data)
docker compose -f docker-compose-airflow.yml down -v
```

### **Data Persistence**
- **MinIO data**: Stored in `./minio_data/` (persists between restarts)
- **Airflow metadata**: Stored in PostgreSQL (persists between restarts)

---

## 📚 **Additional Resources**

- **README.md**: Complete project overview and architecture
- **SQL Scripts**: Located in `trino/sql/` directory (for reference)
- **DAG Files**: Located in `airflow/dags/` directory
- **Configuration**: Docker compose files and environment variables

---

## 🆘 **Support & Troubleshooting**

### **Common Error Messages**
- **"Connection refused"**: Check if services are running and ports are available
- **"DAG not found"**: Ensure DAG files are in the correct directory and syntax is valid
- **"Task failed"**: Check task logs in Airflow UI for specific error details
- **"Spark session error"**: Verify Spark configuration and JAR file availability
- **"MinIO connection error"**: Check MinIO service status and network connectivity

### **Getting Help**
1. Check service logs for error messages
2. Verify Docker container status
3. Ensure all prerequisites are met
4. Check network connectivity between services
5. Review configuration files for syntax errors
6. Verify Python dependencies are installed correctly

---

## 🎯 **Batch Processing Features**

### **Data Quality Features**
- **Great Expectations Integration**: Automated data validation
- **Schema Enforcement**: Data type and format validation
- **Outlier Detection**: Statistical anomaly detection
- **Quality Scoring**: Comprehensive data quality metrics

### **Feature Engineering**
- **Amount Transformations**: Log, percentile, z-score calculations
- **Time Features**: Hour, day-of-week, weekend indicators
- **Statistical Features**: Rolling means, standard deviations
- **Interaction Features**: V-feature and amount combinations

### **ML Pipeline Support**
- **Train/Test Splits**: Automated dataset partitioning
- **Feature Statistics**: Comprehensive feature analysis
- **Parquet Optimization**: Efficient data storage format
- **Batch Scheduling**: Automated pipeline execution

---

**🎉 Congratulations!** You now have a complete, production-ready **batch ETL pipeline** for fraud detection running with Apache Airflow and Apache Spark. This setup provides robust data processing capabilities for building reliable fraud detection systems with automated data quality validation and feature engineering.