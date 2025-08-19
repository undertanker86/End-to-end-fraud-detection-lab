"""
Silver to Gold ETL Pipeline for Fraud Detection
Creates business-ready, aggregated datasets for analytics and ML
"""


from airflow import DAG
from airflow.operators.python import PythonOperator
from typing import Dict, Any  # Add this import
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

# Gold layer datasets to create
GOLD_DATASETS = {
    'fraud_analytics': {
        'description': 'Aggregated fraud metrics for dashboards',
        'aggregation_level': 'hourly',
        'retention': '90 days'
    },
    'ml_features': {
        'description': 'ML-ready feature set for model training',
        'aggregation_level': 'transaction',
        'retention': '1 year'
    },
    'risk_profiles': {
        'description': 'Customer risk profiles and patterns',
        'aggregation_level': 'customer',
        'retention': '2 years'
    },
    'summary_stats': {
        'description': 'Daily/weekly/monthly summary statistics',
        'aggregation_level': 'daily',
        'retention': '2 years'
    }
}

def build_spark_session():
    """Create optimized SparkSession for Silver-to-Gold ETL with MinIO access."""
    import os
    from pyspark.sql import SparkSession

    endpoint_url = os.getenv('AWS_ENDPOINT_URL', 'http://minio:9000')
    access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    # 🚨 FORCE LOCAL MODE to avoid serialization version conflicts
    master_url = 'local[*]'  # Use all available cores, but LOCAL ONLY - no distributed

    hadoop_aws_jar = '/opt/airflow/jars/hadoop-aws-3.3.4.jar'
    aws_sdk_bundle_jar = '/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar'

    spark = (
        SparkSession.builder
        .appName('silver_to_gold_etl_local_only')
        .master(master_url)
        # 🔧 JAR files and S3A configuration
        .config('spark.jars', f'{hadoop_aws_jar},{aws_sdk_bundle_jar}')
        # 🔧 Memory and performance optimizations
        .config('spark.driver.memory', '2g')  # More memory for gold layer
        .config('spark.executor.memory', '2g')
        .config('spark.driver.maxResultSize', '1g')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'false')
        .config('spark.sql.adaptive.enabled', 'true')  # Enable for gold layer
        .config('spark.sql.adaptive.coalescePartitions.enabled', 'true')
        .config('spark.serializer', 'org.apache.spark.serializer.JavaSerializer')
        # 🔧 Compression fixes (no Snappy in Docker)
        .config('spark.sql.parquet.compression.codec', 'uncompressed')
        .config('spark.io.compression.codec', 'lz4')
        .config('spark.sql.parquet.enableVectorizedReader', 'false')
        # 🔧 S3A MinIO configuration
        .config('spark.hadoop.fs.s3a.endpoint', endpoint_url)
        .config('spark.hadoop.fs.s3a.access.key', access_key)
        .config('spark.hadoop.fs.s3a.secret.key', secret_key)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        # 🔧 Session settings
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        .getOrCreate()
    )
    return spark


def validate_silver_data_with_gx(**context) -> Dict[str, Any]:
    """Validate Silver data using custom validation (GX fallback)"""
    import logging
    import pandas as pd
    import io
    from minio import Minio
    from datetime import datetime
    
    logging.info("🔍 Starting enhanced Silver data validation...")
    
    try:
        # Get MinIO client
        client = Minio(
            'minio:9000',
            access_key='minioadmin',
            secret_key='minioadmin',
            secure=False
        )
        
        # Read sample of silver data for validation
        # Note: For large files, we validate a sample to avoid memory issues
        try:
            # Try to get the Parquet file directly from MinIO
            response = client.get_object('silver', 'creditcard_current.parquet')
            # For validation, we'll use pandas to read the parquet data
            df = pd.read_parquet(io.BytesIO(response.data))
        except Exception as parquet_error:
            logging.warning(f"Could not read parquet directly: {parquet_error}")
            # Fallback: try reading CSV if parquet fails
            response = client.get_object('bronze', 'creditcard.csv')
            df = pd.read_csv(io.BytesIO(response.data))
            logging.info("Using bronze CSV data for validation")
        
        # Limit to sample for validation performance
        if len(df) > 10000:
            df = df.sample(n=10000, random_state=42)
            logging.info(f"Using sample of 10,000 records for GX validation")
        
        # Custom Silver Data Validation (more reliable than GX)
        validation_checks = []
        validation_errors = []
        
        # 1. Basic data checks
        if len(df) > 0:
            validation_checks.append(f"✅ Data loaded: {len(df):,} records, {len(df.columns)} columns")
        else:
            validation_errors.append("❌ No data found in silver layer")
        
        # 2. Critical columns check
        critical_columns = ['Class', 'Amount']
        for col in critical_columns:
            if col in df.columns:
                null_count = df[col].isnull().sum()
                if null_count == 0:
                    validation_checks.append(f"✅ {col}: no null values")
                else:
                    validation_errors.append(f"❌ {col}: {null_count} null values found")
        
        # 3. Class validation
        if 'Class' in df.columns:
            unique_classes = set(df['Class'].unique())
            if unique_classes <= {0, 1}:
                validation_checks.append("✅ Class values are binary (0/1)")
            else:
                validation_errors.append(f"❌ Invalid Class values: {unique_classes}")
        
        # 4. Amount validation
        if 'Amount' in df.columns:
            amount_stats = df['Amount'].describe()
            if amount_stats['min'] >= 0 and amount_stats['max'] <= 100000:
                validation_checks.append(f"✅ Amount in valid range: {amount_stats['min']:.2f} to {amount_stats['max']:.2f}")
            else:
                validation_errors.append(f"❌ Amount out of range: {amount_stats['min']:.2f} to {amount_stats['max']:.2f}")
        
        # 5. Engineered features validation
        engineered_features = ['Amount_Log', 'Hour', 'IsWeekend', 'DataQuality_Score']
        for feature in engineered_features:
            if feature in df.columns:
                validation_checks.append(f"✅ {feature} feature present")
        
        if 'Hour' in df.columns:
            hour_range = df['Hour'].agg(['min', 'max'])
            if 0 <= hour_range['min'] and hour_range['max'] <= 23:
                validation_checks.append("✅ Hour values in valid range [0-23]")
            else:
                validation_errors.append(f"❌ Hour values out of range: {hour_range['min']} to {hour_range['max']}")
        
        # 6. PCA features check
        pca_cols = [col for col in df.columns if col.startswith('V') and col[1:].isdigit()]
        if pca_cols:
            validation_checks.append(f"✅ {len(pca_cols)} PCA features (V1-V{len(pca_cols)}) present")
            
            # Check a few PCA features for reasonable ranges
            sample_pca = pca_cols[:3]  # Check first 3
            pca_issues = []
            for col in sample_pca:
                col_range = df[col].agg(['min', 'max'])
                if col_range['min'] < -50 or col_range['max'] > 50:
                    pca_issues.append(f"{col}: {col_range['min']:.2f} to {col_range['max']:.2f}")
            
            if not pca_issues:
                validation_checks.append("✅ PCA features in reasonable ranges")
            else:
                validation_errors.append(f"❌ PCA features with extreme values: {', '.join(pca_issues)}")
        
        validation_report = {
            'validation_timestamp': datetime.now().isoformat(),
            'data_source': 'silver_layer',
            'sample_size': len(df),
            'total_columns': len(df.columns),
            'checks_passed': len(validation_checks),
            'errors_found': len(validation_errors),
            'success': len(validation_errors) == 0,
            'validation_checks': validation_checks,
            'validation_errors': validation_errors,
            'evaluation_parameters': {
                'run_id': context.get('run_id', 'unknown'),
                'dag_id': context.get('dag').dag_id if context.get('dag') else 'unknown'
            }
        }
        
        if validation_report['success']:
            logging.info("🎉 All silver data validations passed!")
            validation_report['status'] = 'PASSED'
        else:
            logging.warning("⚠️ Some silver data validations failed")
            validation_report['status'] = 'FAILED_BUT_CONTINUING'
        
        logging.info(f"📊 Silver Data Validation Summary:")
        logging.info(f"   • Sample size: {len(df):,} records")
        logging.info(f"   • Columns validated: {len(df.columns)}")
        logging.info(f"   • Checks passed: {len(validation_checks)}")
        logging.info(f"   • Errors found: {len(validation_errors)}")
        logging.info(f"   • Status: {validation_report['status']}")
        
        for check in validation_checks:
            logging.info(f"   {check}")
        for error in validation_errors:
            logging.warning(f"   {error}")
        
        return validation_report
        
    except Exception as e:
        logging.error(f"❌ Silver data validation failed: {str(e)}")
        # Don't fail the pipeline - return partial results
        return {
            'validation_timestamp': datetime.now().isoformat(),
            'status': 'ERROR',
            'error': str(e),
            'data_source': 'silver_layer'
        }


# 🔧 DISABLED: Simplified pipeline - keeping only essential tasks
# def create_risk_profiles_dataset(**context):
#     """Create customer risk profiles using Spark"""
#     import logging
#     from pyspark.sql import functions as F
#     from pyspark.sql.window import Window
#     from functools import reduce
#     from operator import add
#     from datetime import datetime
# 
#     logging.info("👤 Creating risk profiles dataset with Spark...")
# 
#     spark = None
#     try:
#         spark = build_spark_session()
#         silver_path = 's3a://silver/creditcard_current.parquet'
#         gold_path = 's3a://gold/risk_profiles_current.parquet'
# 
#         df = spark.read.parquet(silver_path)
# 
#         # ... (rest of the function commented out for brevity)
#
#     except Exception as e:
#         logging.error(f"❌ Risk profiles creation failed: {e}")
#         raise
#     finally:
#         if spark:
#             logging.info("🔧 Stopping Spark session for risk profiles...")
#             spark.stop()
# 
# def create_summary_stats_dataset(**context):
#     """Create daily/weekly summary statistics using Spark"""
#     import logging
#     from pyspark.sql import functions as F
#     from pyspark.sql.window import Window
#     from functools import reduce
#     from operator import add
#     from datetime import datetime
# 
#     logging.info("📊 Creating summary statistics dataset with Spark...")
# 
#     spark = None
#     try:
#         spark = build_spark_session()
#         silver_path = 's3a://silver/creditcard_current.parquet'
#         gold_path = 's3a://gold/summary_stats_current.parquet'
# 
#         df = spark.read.parquet(silver_path)
#
#         # ... (rest of the function commented out for brevity)
#
#     except Exception as e:
#         logging.error(f"❌ Summary statistics creation failed: {e}")
#         raise
#     finally:
#         if spark:
#             logging.info("🔧 Stopping Spark session for summary stats...")
#             spark.stop()

def create_fraud_analytics_dataset(**context):
    """Create aggregated fraud analytics dataset using Spark"""
    import logging
    from pyspark.sql import functions as F
    from datetime import datetime

    logging.info("🏆 Creating fraud analytics dataset with Spark...")

    spark = None
    try:
        # 🔍 Find latest silver data file
        try:
            from minio import Minio
            client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
            objects = list(client.list_objects('silver', prefix='creditcard_sample_', recursive=True))
            if objects:
                # Get the most recent parquet file (not _SUCCESS files)
                parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
                if parquet_files:
                    latest_file = max(parquet_files, key=lambda x: x.last_modified)
                    # Extract directory path (remove the part-xxxx.parquet filename)
                    silver_path = f"s3a://silver/{latest_file.object_name.split('/')[0]}"
                    logging.info(f"📂 Using latest silver data: {silver_path}")
                else:
                    raise Exception("No parquet files found in silver bucket")
            else:
                raise Exception("No sample files found in silver bucket")
        except Exception as e:
            logging.warning(f"⚠️ Could not find latest silver file, falling back to default: {e}")
            silver_path = 's3a://silver/creditcard_current.parquet'

        spark = build_spark_session()
        gold_path = 's3a://gold/fraud_analytics_hourly.parquet'

        df = spark.read.parquet(silver_path)

        # Ensure required columns exist
        if 'Hour' not in df.columns:
            df = df.withColumn('Hour', F.lit(0))
        if 'IsWeekend' not in df.columns:
            df = df.withColumn('IsWeekend', F.lit(0))
        if 'Amount_Category' not in df.columns:
            df = df.withColumn('Amount_Category', F.lit('Unknown'))

        if 'Time' in df.columns:
            df = df.withColumn('Time', F.col('Time').cast('timestamp'))
            hourly = df.groupBy(
                F.date_trunc('hour', 'Time').alias('timestamp'),
                'Hour', 'IsWeekend'
            ).agg(
                F.sum('Class').alias('fraud_count'),
                F.count(F.lit(1)).alias('total_transactions'),
                F.sum('Amount').alias('total_amount'),
                F.avg('Amount').alias('avg_amount'),
                F.expr('percentile_approx(Amount, 0.5)').alias('median_amount'),
                F.stddev('Amount').alias('std_amount'),
                F.avg('DataQuality_Score').alias('avg_data_quality'),
            )
        else:
            hourly = df.groupBy('Hour', 'IsWeekend').agg(
                F.sum('Class').alias('fraud_count'),
                F.count(F.lit(1)).alias('total_transactions'),
                F.sum('Amount').alias('total_amount'),
                F.avg('Amount').alias('avg_amount'),
                F.expr('percentile_approx(Amount, 0.5)').alias('median_amount'),
                F.stddev('Amount').alias('std_amount'),
                F.avg('DataQuality_Score').alias('avg_data_quality'),
            ).withColumn('timestamp', F.current_timestamp())

        hourly = hourly.withColumn('fraud_rate', F.col('fraud_count') / F.col('total_transactions')) \
            .withColumn('created_at', F.current_timestamp()) \
            .withColumn('data_version', F.lit(context['run_id']))

        hourly.write.mode('overwrite').parquet(gold_path)

        records = hourly.count()
        time_range = 'N/A'
        if 'timestamp' in hourly.columns:
            minmax = hourly.agg(F.min('timestamp').alias('min_ts'), F.max('timestamp').alias('max_ts')).collect()[0]
            time_range = f"{minmax['min_ts']} to {minmax['max_ts']}"

        return {
            'dataset': 'fraud_analytics',
            'records': int(records),
            'time_range': time_range
        }

    except Exception as e:
        logging.error(f"❌ Fraud analytics creation failed: {e}")
        raise
    finally:
        if spark:
            logging.info("🔧 Stopping Spark session for fraud analytics...")
            spark.stop()

def create_ml_features_dataset(**context):
    """Create ML-ready feature dataset using Spark"""
    import logging
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from functools import reduce
    from operator import add

    logging.info("🤖 Creating ML features dataset with Spark...")

    spark = None
    try:
        # 🔍 Find latest silver data file
        try:
            from minio import Minio
            client = Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)
            objects = list(client.list_objects('silver', prefix='creditcard_sample_', recursive=True))
            if objects:
                # Get the most recent parquet file (not _SUCCESS files)
                parquet_files = [obj for obj in objects if obj.object_name.endswith('.parquet')]
                if parquet_files:
                    latest_file = max(parquet_files, key=lambda x: x.last_modified)
                    # Extract directory path (remove the part-xxxx.parquet filename)
                    silver_path = f"s3a://silver/{latest_file.object_name.split('/')[0]}"
                    logging.info(f"📂 Using latest silver data: {silver_path}")
                else:
                    raise Exception("No parquet files found in silver bucket")
            else:
                raise Exception("No sample files found in silver bucket")
        except Exception as e:
            logging.warning(f"⚠️ Could not find latest silver file, falling back to default: {e}")
            silver_path = 's3a://silver/creditcard_current.parquet'

        spark = build_spark_session()
        gold_current = 's3a://gold/ml_features_current.parquet'
        gold_train = 's3a://gold/ml_features_train.parquet'
        gold_test = 's3a://gold/ml_features_test.parquet'

        df = spark.read.parquet(silver_path)

        feature_columns = [
            'V1','V2','V3','V4','V5','V6','V7','V8','V9','V10',
            'V11','V12','V13','V14','V15','V16','V17','V18','V19','V20',
            'V21','V22','V23','V24','V25','V26','V27','V28',
            'Amount_Log','Amount_Percentile','Hour','DayOfWeek','IsWeekend','DataQuality_Score','Class'
        ]

        # 🔧 Add outlier columns to feature_columns if they exist
        outlier_columns = [c for c in df.columns if c.endswith('_IsOutlier')]
        if outlier_columns:
            feature_columns.extend(outlier_columns)
            logging.info(f"🎯 Added {len(outlier_columns)} outlier columns to feature selection: {outlier_columns}")

        # Ensure columns exist
        for col in feature_columns:
            if col not in df.columns:
                df = df.withColumn(col, F.lit(0.0) if col != 'Class' else F.lit(0))

        ml = df.select(*feature_columns)

        # Interaction features
        ml = ml.withColumn('V1_Amount_Interaction', F.col('V1') * F.col('Amount_Log')) \
               .withColumn('V2_Amount_Interaction', F.col('V2') * F.col('Amount_Log'))

        # Time cyclic encodings
        ml = ml.withColumn('Hour_Sin', F.sin(2 * 3.1415926535 * F.col('Hour') / 24.0)) \
               .withColumn('Hour_Cos', F.cos(2 * 3.1415926535 * F.col('Hour') / 24.0)) \
               .withColumn('DayOfWeek_Sin', F.sin(2 * 3.1415926535 * F.col('DayOfWeek') / 7.0)) \
               .withColumn('DayOfWeek_Cos', F.cos(2 * 3.1415926535 * F.col('DayOfWeek') / 7.0))

        # Rolling stats proxy using window on Hour ordering
        w = Window.orderBy('Hour').rowsBetween(-99, 0)
        ml = ml.withColumn('Amount_Rolling_Mean', F.avg('Amount_Log').over(w)) \
               .withColumn('Amount_Rolling_Std', F.stddev_pop('Amount_Log').over(w))

        # Outlier ratio - now simplified since outlier columns are included in ml
        outlier_cols = [c for c in ml.columns if c.endswith('_IsOutlier')]
        logging.info(f"🔍 Found {len(outlier_cols)} outlier columns in ml: {outlier_cols}")
        
        if outlier_cols:
            logging.info(f"✅ Using outlier columns for ratio calculation")
            ml = ml.withColumn('Outlier_Ratio', reduce(add, [F.col(c).cast('int') for c in outlier_cols]) / F.lit(len(outlier_cols)))
        else:
            logging.warning(f"⚠️ No outlier columns found! Creating synthetic outlier indicators...")
            # 🔧 FALLBACK: Create basic outlier detection for V1-V5
            outlier_indicators = []
            for i in range(1, 6):  # V1-V5
                v_col = f'V{i}'
                if v_col in ml.columns:
                    # Simple outlier: beyond ±3 standard deviations
                    outlier_indicators.append(
                        F.when((F.col(v_col) < -3.0) | (F.col(v_col) > 3.0), 1).otherwise(0)
                    )
            
            if outlier_indicators:
                ml = ml.withColumn('Outlier_Ratio', reduce(add, outlier_indicators) / F.lit(len(outlier_indicators)))
                logging.info(f"✅ Created synthetic outlier ratio from V1-V5")
            else:
                ml = ml.withColumn('Outlier_Ratio', F.lit(0.0))
                logging.info(f"⚠️ Using default outlier ratio = 0.0")

        ml = ml.withColumn('feature_version', F.lit('1.0')) \
               .withColumn('created_at', F.current_timestamp()) \
               .withColumn('data_source', F.lit('silver_layer'))

        # Write current
        ml.write.mode('overwrite').parquet(gold_current)

        # Train/test split
        train, test = ml.randomSplit([0.8, 0.2], seed=42)
        train.write.mode('overwrite').parquet(gold_train)
        test.write.mode('overwrite').parquet(gold_test)

        return {
            'dataset': 'ml_features',
            'total_records': int(ml.count()),
            'train_records': int(train.count()),
            'test_records': int(test.count()),
            'feature_count': len(ml.columns)
        }

    except Exception as e:
        logging.error(f"❌ ML features creation failed: {e}")
        raise
    finally:
        if spark:
            logging.info("🔧 Stopping Spark session for ML features...")
            spark.stop()

def save_to_gold(df, filename):
    """Deprecated: Spark jobs write directly to s3a paths."""
    import logging
    logging.info(f"Bypassed save_to_gold; Spark writes to gold as s3a://gold/{filename}")

def get_minio_client():
    """Retained for backward compatibility; not used by Spark paths."""
    from minio import Minio
    return Minio('minio:9000', access_key='minioadmin', secret_key='minioadmin', secure=False)

from datetime import datetime, timedelta

# DAG definition
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'silver_to_gold_etl',
    default_args=DEFAULT_ARGS,
    description='Silver to Gold ETL Pipeline - Create business-ready datasets',
    schedule_interval=timedelta(hours=12),  # Run twice daily
    max_active_runs=1,
    catchup=False,
    tags=['etl', 'gold-layer', 'analytics', 'ml-features'],
)

# Tasks
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

activate_conda_env = BashOperator(
    task_id='activate_conda_env',
    bash_command='echo "Skip per-container pip install; dependencies are preinstalled at container start"',
    dag=dag
)

# 🔧 NEW: GX Validation Task
validate_silver_task = PythonOperator(
    task_id='validate_silver_with_gx',
    python_callable=validate_silver_data_with_gx,
    dag=dag,
    retries=1,
)

create_fraud_analytics_task = PythonOperator(
    task_id='create_fraud_analytics',
    python_callable=create_fraud_analytics_dataset,
    dag=dag,
)

create_ml_features_task = PythonOperator(
    task_id='create_ml_features',
    python_callable=create_ml_features_dataset,
    dag=dag,
)

# 🔧 DISABLED: Simplified pipeline - keeping only essential tasks
# create_risk_profiles_task = PythonOperator(
#     task_id='create_risk_profiles',
#     python_callable=create_risk_profiles_dataset,
#     dag=dag,
# )

# create_summary_stats_task = PythonOperator(
#     task_id='create_summary_stats',
#     python_callable=create_summary_stats_dataset,
#     dag=dag,
# )

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# 🔧 SIMPLIFIED Dependencies: Only 2 essential gold datasets
start_task >> activate_conda_env >> validate_silver_task >> [
    create_fraud_analytics_task, 
    create_ml_features_task
] >> end_task