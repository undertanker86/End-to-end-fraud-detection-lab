# 🚀 Fraud Detection Project - Complete Data Pipeline

> **End-to-end fraud detection lab using Bronze/Silver/Gold lakehouse layers with batch (Spark/Airflow) and streaming (Kafka/PyFlink), stored in MinIO, queried by Trino.**

[![Docker](https://img.shields.io/badge/Docker-Required-blue.svg)](https://docker.com)
[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.2-green.svg)](https://airflow.apache.org)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-3.5.1-orange.svg)](https://spark.apache.org)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-7.5.0-purple.svg)](https://kafka.apache.org)
[![PyFlink](https://img.shields.io/badge/PyFlink-1.17.0-yellow.svg)](https://flink.apache.org)
[![MinIO](https://img.shields.io/badge/MinIO-Object%20Storage-red.svg)](https://min.io)
[![Trino](https://img.shields.io/badge/Trino-SQL%20Engine-green.svg)](https://trino.io)

---

## 📋 **Table of Contents**

- [🎯 Project Overview](#-project-overview)
- [🏗️ Architecture](#️-architecture)
- [🛠️ Tech Stack](#️-tech-stack)
- [📁 Project Structure](#-project-structure)
- [🚀 Quick Start](#-quick-start)
- [📊 Data Pipeline](#-data-pipeline)
- [🔧 Configuration](#-configuration)
- [📈 Usage Examples](#-usage-examples)
- [🆘 Troubleshooting](#-troubleshooting)
- [📚 Documentation](#-documentation)
- [🔒 Security & Privacy](#-security--privacy)

---

## 🎯 **Project Overview**

This project demonstrates a **production-ready fraud detection system** that combines **batch processing** and **real-time streaming** to detect fraudulent credit card transactions. The system processes transaction data through multiple layers, applying machine learning features and statistical analysis to identify potential fraud patterns.

### **Key Features**
- 🔄 **Batch ETL**: Apache Airflow + Spark for historical data processing
- ⚡ **Real-time Streaming**: Apache Kafka + PyFlink for live transaction monitoring + Rolling window
- 🗄️ **Data Lakehouse**: Bronze/Silver/Gold architecture with MinIO storage
- 🔍 **ML Features**: Automated feature engineering for fraud detection models
- 📊 **Data Quality**: Great Expectations integration for data validation
- 🚀 **Scalable**: Docker-based deployment with microservices architecture

---

## 🏗️ **Architecture**
[![Over](https://res.cloudinary.com/dptjhpkmv/image/upload/v1755590563/FSDS-2-Project.drawio_1_cwvmi4.png)]
```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           FRAUD DETECTION PIPELINE                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Raw Data  │    │   Bronze    │    │   Silver    │    │    Gold     │  │
│  │   (CSV)     │───▶│   (Raw)     │───▶│ (Cleaned)   │───▶│ (ML Ready)  │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                   │                   │                   │      │
│         ▼                   ▼                   ▼                   ▼      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │   Upload    │    │ Validation  │    │ Feature     │    │   ML        │  │
│  │   Script    │    │   (GX)      │    │ Engineering │    │ Features    │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                           STREAMING PIPELINE                                │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │ PostgreSQL  │    │    Kafka    │    │   PyFlink   │    │   MinIO     │  │
│  │   (CDC)     │───▶│   (Events)  │───▶│ (Streaming) │───▶│ (JSON)      │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                   │                   │                   │      │
│         ▼                   ▼                   ▼                   ▼      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │  Debezium   │    │ Schema      │    │ Real-time   │    │ Streaming   │  │
│  │ Connector   │    │ Registry    │    │ Features    │    │ Analytics   │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│                                                                             │
├─────────────────────────────────────────────────────────────────────────────┤
│                           QUERY & ANALYTICS                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │    MinIO    │    │    Trino    │    │   DBeaver   │    │   Python    │  │
│  │ (Storage)   │───▶│ (SQL Engine)│───▶│ (GUI Client)│───▶│ (Analysis)  │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
│         │                   │                   │                   │      │
│         ▼                   ▼                   ▼                   ▼      │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐  │
│  │ Parquet     │    │ SQL         │    │ Visual      │    │ ML Model    │  │
│  │ Files       │    │ Queries     │    │ Analytics   │    │ Training    │  │
│  └─────────────┘    └─────────────┘    └─────────────┘    └─────────────┘  │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## 🛠️ **Tech Stack**

### **Core Technologies**
| Component | Technology | Version | Purpose |
|-----------|------------|---------|---------|
| **Orchestration** | Apache Airflow | 2.9.2 | Batch ETL pipeline orchestration |
| **Batch Processing** | Apache Spark | 3.5.1 | Large-scale data processing |
| **Stream Processing** | Apache Kafka | 7.5.0 | Real-time event streaming |
| **Stream Processing** | PyFlink | 1.17.0 | Real-time feature engineering |
| **Storage** | MinIO | Latest | Object storage (S3-compatible) |
| **Query Engine** | Trino | Latest | Distributed SQL query engine |
| **Data Quality** | Great Expectations | 0.17.23 | Data validation and testing |

### **Infrastructure**
| Service | Technology | Purpose |
|---------|------------|---------|
| **Message Broker** | Apache Kafka | Event streaming and CDC |
| **Schema Registry** | Confluent Schema Registry | Data schema management |
| **Database** | PostgreSQL | Airflow metadata and CDC source |
| **Cache** | Redis | Airflow Celery broker |
| **Monitoring** | Kafka Control Center | Stream monitoring and management |

---

## 📁 **Project Structure**

```
Fraud-Detection-Project/
├── 📁 airflow/                          # Apache Airflow configuration
│   ├── 📁 dags/                         # ETL pipeline DAGs
│   │   ├── bronze_to_silver_etl.py      # Bronze → Silver ETL pipeline
│   │   └── silver_to_gold_etl.py        # Silver → Gold ETL pipeline
│   ├── 📁 logs/                         # Airflow execution logs
│   ├── 📁 plugins/                      # Custom Airflow plugins
│   └── requirements.txt                 # Python dependencies
│
├── 📁 data_ingestion/                   # Streaming data ingestion
│   └── 📁 kafka_producer/               # PyFlink streaming jobs
│       ├── 📁 pyflink_jobs/             # Stream processing logic
│       │   ├── fraud_stream_processor.py # Main streaming job
│       │   └── fraud_stream_simple_test.py # Test streaming job
│       ├── 📁 avro_schemas/             # Data schemas
│       ├── Dockerfile                   # Streaming container
│       └── requirements.txt             # Streaming dependencies
│
├── 📁 trino/                            # Trino query engine
│   ├── 📁 etc/                          # Coordinator configuration
│   ├── 📁 etc-worker/                   # Worker configuration
│   ├── 📁 sql/                          # SQL scripts and queries
│   │   ├── create_schemas.sql           # Schema creation
│   │   ├── create_gold_tables.sql       # Gold layer tables
│   │   ├── analytics_queries.sql        # Analytics queries
│   │   └── gold_layer_analytics.sql     # Gold layer analytics
│   └── 📁 data/                         # Trino data storage
│
├── 📁 configs/                          # Configuration templates
│   └── postgresql-cdc.json              # PostgreSQL CDC connector config
│
├── 📁 stream/                           # Stream simulation & management
│   ├── run.sh                           # Stream management script
│   └── stream_simulator.py              # Stream data simulator
│
├── 📁 raw-data/                         # Source data files
├── 📁 minio_data/                       # MinIO data storage
├── 📁 kafka_data/                       # Kafka data storage
├── 📁 zookeeper_data/                   # Zookeeper data
│
├── 📁 jars/                             # Required JAR files
├── 📁 trino/logs/                       # Trino logs
├── 📁 airflow/logs/                     # Airflow logs
│
├── 📄 docker-compose.yml                # Core services (MinIO, Kafka, etc.)
├── 📄 docker-compose-airflow.yml        # Airflow stack
├── 📄 docker-compose-trino.yml          # Trino stack
├── 📄 start_airflow_pipeline.sh         # Easy startup script
├── 📄 upload_to_bronze.py               # Bronze data uploader
│
├── 📄 AIRFLOW_ETL_SETUP.md              # Airflow ETL setup guide
├── 📄 README_TRINO_MINIO.md             # Trino + MinIO configuration guide
└── 📄 README.md                         # This file
```

---

## 🚀 **Quick Start**

### **Prerequisites**
- Docker & Docker Compose
- 8GB+ RAM available
- 20GB+ disk space
- Linux/macOS/Windows with Docker support

### **1. Clone & Setup**
```bash
git clone <your-repo>
cd Fraud-Detection-Project
chmod +x start_airflow_pipeline.sh
```

### **3. Manual Startup **
```bash
# Start core infrastructure
docker compose up -d

# Start Airflow stack
docker compose -f docker-compose-airflow.yml up -d

# Start Trino
docker compose -f docker-compose-trino.yml up -d
```

### **4. Access Services**
| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin | Object storage management |
| **Kafka Control Center** | http://localhost:9021 | - | Stream monitoring |
| **Trino** | localhost:8090 | any user | SQL queries |

---

## 📊 **Data Pipeline**

### **Batch Processing (Airflow + Spark)**
```
1. Raw Data Upload → Bronze Layer
   ├── upload_to_bronze.py
   ├── Data validation with Great Expectations
   └── Schema enforcement

2. Bronze → Silver ETL
   ├── bronze_to_silver_etl.py DAG
   ├── Data cleaning and validation
   ├── Feature engineering
   └── Quality scoring

3. Silver → Gold ETL
   ├── silver_to_gold_etl.py DAG
   ├── ML feature engineering
   ├── Dataset splitting (train/test)
   └── Parquet file generation
```

### **Streaming Processing (Kafka + PyFlink)**
```
1. CDC Events → Kafka
   ├── Debezium connector for PostgreSQL
   ├── Schema registry for data consistency
   └── Topic partitioning

2. Kafka → PyFlink Processing
   ├── Real-time feature engineering
   ├── Window-based aggregations
   ├── ML feature updates
   └── MinIO storage (JSON)

3. Streaming Analytics
   ├── Hourly bucketing
   ├── Feature aggregation
   └── Real-time fraud detection
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
├── Parquet files                 # Optimized batch data
└── JSON streaming                # Real-time features
```

---

## 🔧 **Configuration**

### **Environment Variables**
Create a `.env` file (do not commit real secrets):
```bash
# MinIO Configuration
MINIO_ENDPOINT=http://localhost:9000
MINIO_ROOT_USER=minioadmin
MINIO_ROOT_PASSWORD=minioadmin
MINIO_BUCKET=gold

# Kafka Configuration
KAFKA_BROKER=localhost:9092

# PostgreSQL Configuration
POSTGRES_HOST=localhost
POSTGRES_PORT=5434
POSTGRES_DB=fraud_detection
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres

# Trino Configuration
TRINO_HOST=localhost
TRINO_PORT=8090
TRINO_CATALOG=minio
TRINO_SCHEMA=gold_ml
```

### **Key Configuration Files**
- **Airflow**: `docker-compose-airflow.yml`
- **Trino**: `trino/etc/catalog/minio.properties`
- **Kafka**: `docker-compose.yml`
- **CDC**: `configs/postgresql-cdc.json`

---

## 📈 **Usage Examples**

### **1. Execute Batch ETL Pipelines**
```bash
# Trigger DAGs manually
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags trigger bronze_to_silver_etl

docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags trigger silver_to_gold_etl
```

### **2. Start Streaming Pipeline**
```bash
# Submit PyFlink job
docker exec -it flink-jobmanager flink run -py \
  /opt/flink/pyflink_jobs/fraud_stream_processor.py

# Register CDC connector
bash stream/run.sh register_connector configs/postgresql-cdc.json

# Simulate streaming data
python3 stream/stream_simulator.py
```

### **3. Query Data with Trino**
```sql
-- Check available schemas
SHOW SCHEMAS IN minio;

-- Query ML features
SELECT COUNT(*) FROM minio.gold_ml.ml_features_current;
SELECT class, COUNT(*) FROM minio.gold_ml.ml_features_train GROUP BY class;

-- Streaming data
SELECT * FROM minio.gold_streaming.ml_features_stream_parsed 
ORDER BY hour_bucket DESC LIMIT 10;
```

### **4. Python Analysis**
```python
# Direct MinIO access
from minio import Minio
import pandas as pd

client = Minio('localhost:9000', 
               access_key='minioadmin', 
               secret_key='minioadmin', 
               secure=False)

# Load ML features
response = client.get_object('gold', 'ml_features_current.parquet/part-00000-*.parquet')
df = pd.read_parquet(io.BytesIO(response.data))

print(f"Dataset: {len(df):,} records")
print(f"Fraud Rate: {df['Class'].mean()*100:.4f}%")
```

---

## 🆘 **Troubleshooting**

### **Common Issues & Solutions**

#### **1. Service Startup Issues**
```bash
# Check service status
docker compose -f docker-compose-airflow.yml ps
docker compose ps
docker compose -f docker-compose-trino.yml ps

# Check service logs
docker compose -f docker-compose-airflow.yml logs -f airflow-scheduler-1
docker compose -f docker-compose-airflow.yml logs -f airflow-worker-1
```

#### **2. Airflow DAG Issues**
```bash
# List all DAGs
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags list

# Unpause paused DAGs
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow dags unpause silver_to_gold_etl

# Clear failed tasks
docker compose -f docker-compose-airflow.yml exec airflow-scheduler-1 \
  airflow tasks clear bronze_to_silver_etl --task-regex "upload_to_silver" --yes
```

#### **3. Dependency Issues**
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

#### **4. Network Connectivity**
```bash
# Check network connectivity
docker network ls
docker network inspect fraud-detection-project_fraud-detection-network

# Connect containers to network if needed
docker network connect fraud-detection-project_fraud-detection-network \
  fraud-detection-project-trino-coordinator-1
```

### **Error Messages & Solutions**
- **"Connection refused"**: Check if services are running and ports are available
- **"DAG not found"**: Ensure DAG files are in the correct directory and syntax is valid
- **"Task failed"**: Check task logs in Airflow UI for specific error details
- **"Network error"**: Verify Docker network configuration and container connectivity
- **"Spark session error"**: Verify Spark configuration and JAR file availability

---

## 📚 **Documentation**

### **Guides & References**README_TRINO_MINIO.md
- **[AIRFLOW_ETL_SETUP.md](AIRFLOW_ETL_SETUP.md)**: Comprehensive Airflow ETL setup guide
- **[README_TRINO_MINIO.md](README_TRINO_MINIO.md)**: Detailed Trino + MinIO configuration

### **Key Directories**
- **SQL Scripts**: `trino/sql/` - Database setup and analytics queries
- **Streaming Jobs**: `data_ingestion/kafka_producer/pyflink_jobs/` - Real-time processing
- **Airflow DAGs**: `airflow/dags/` - ETL pipeline definitions
- **Configuration**: `configs/` - Service configuration templates

---


## 🎯 **Project Goals & Use Cases**

### **Primary Objectives**
1. **Demonstrate** modern data engineering practices
2. **Implement** production-ready fraud detection pipelines
3. **Showcase** batch and streaming data processing
4. **Provide** hands-on experience with big data technologies
5. **Enable** ML model development and training

### **Target Use Cases**
- **Financial Services**: Credit card fraud detection
- **E-commerce**: Transaction monitoring and fraud prevention
- **Banking**: Real-time fraud alerting systems
- **Insurance**: Claims fraud detection
- **Education**: Data engineering and ML learning platform

---

## 🤝 **Contributing & Support**

### **Getting Help**
1. Check service logs for error messages
2. Verify Docker container status
3. Ensure all prerequisites are met
4. Check network connectivity between services
5. Review configuration files for syntax errors
6. Verify Python dependencies are installed correctly

### **Development Workflow**
1. **Setup**: Follow the Quick Start guide
2. **Development**: Modify DAGs, add new features
3. **Testing**: Test locally with Docker
4. **Documentation**: Update relevant guides
5. **Deployment**: Use provided scripts

---

## 📄 **License & Credits**

- **Educational Use**: This project is designed for educational and learning purposes
- **Open Source**: Built with open-source technologies
- **Licenses**: Review licenses for bundled connectors/jars before publishing
- **Credits**: Thanks to Apache projects (Airflow, Kafka, Flink, Spark), Trino, and MinIO

---

## 🌟 **Features Summary**

| Feature | Description | Technology |
|---------|-------------|------------|
| **Batch ETL** | Automated data processing pipelines | Airflow + Spark |
| **Real-time Streaming** | Live transaction monitoring | Kafka + PyFlink |
| **Data Lakehouse** | Multi-layer data architecture | MinIO + Parquet |
| **ML Features** | Automated feature engineering | Python + Spark |
| **Data Quality** | Validation and testing | Great Expectations |
| **SQL Analytics** | Distributed query processing | Trino |
| **Monitoring** | Pipeline and stream monitoring | Airflow UI + Kafka Control Center |
| **Scalability** | Containerized microservices | Docker + Docker Compose |

---

---

*For detailed setup instructions, see the specific guides in the documentation section above, and if you want get other resource contact me through taduong.work@gmail.com*
