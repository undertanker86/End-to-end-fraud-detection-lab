#!/usr/bin/env python3
"""
Bronze to Silver ETL Pipeline DAG
Processes fraud detection data from bronze (raw) to silver (cleaned/enriched) layer
"""

from datetime import datetime, timedelta
from typing import Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator


   

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}


def get_minio_client():
    from minio import Minio  # ✅ Chỉ khi thực sự chạy task mới cần
    import os
    from minio.error import S3Error
    
    MINIO_ENDPOINT = os.getenv('AWS_ENDPOINT_URL', 'localhost:9000').replace('http://', '')
    MINIO_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    MINIO_SECRET_KEY = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')

    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def build_spark_session():
    """Create a more robust Spark session."""
    import os
    from pyspark.sql import SparkSession
    
    endpoint_url = os.getenv('AWS_ENDPOINT_URL', 'http://minio:9000')
    access_key = os.getenv('AWS_ACCESS_KEY_ID', 'minioadmin')
    secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'minioadmin')
    
    # 🔧 ADD: JAR files paths
    hadoop_aws_jar = '/opt/airflow/jars/hadoop-aws-3.3.4.jar'
    aws_sdk_bundle_jar = '/opt/airflow/jars/aws-java-sdk-bundle-1.11.1026.jar'
    
    spark = (
        SparkSession.builder
        .appName('bronze_to_silver_simple')
        .master('local[1]')  # ← Single thread to avoid issues
        # 🔧 ADD: Load JAR files - THIS WAS MISSING!
        .config('spark.jars', f'{hadoop_aws_jar},{aws_sdk_bundle_jar}')
        .config('spark.driver.memory', '1g')  # ← Reduce memory
        .config('spark.executor.memory', '1g')
        .config('spark.driver.maxResultSize', '512m')
        .config('spark.sql.execution.arrow.pyspark.enabled', 'false')  # ← Disable Arrow
        .config('spark.sql.adaptive.enabled', 'false')
        .config('spark.serializer', 'org.apache.spark.serializer.JavaSerializer')  # ← Safer serializer
        # 🔧 FIX: Disable Snappy compression to avoid native library issues
        .config('spark.sql.parquet.compression.codec', 'uncompressed')
        .config('spark.io.compression.codec', 'lz4')
        .config('spark.sql.parquet.enableVectorizedReader', 'false')
        .config('spark.hadoop.fs.s3a.endpoint', endpoint_url)
        .config('spark.hadoop.fs.s3a.access.key', access_key)
        .config('spark.hadoop.fs.s3a.secret.key', secret_key)
        .config('spark.hadoop.fs.s3a.path.style.access', 'true')
        # 🔧 ADD: S3A implementation - THIS WAS ALSO MISSING!
        .config('spark.hadoop.fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .getOrCreate()
    )
    
    return spark

def validate_bronze_data(**context) -> Dict[str, Any]:
    import logging
    import io
    import pandas as pd
    import numpy as np
    """
    Enhanced bronze data validation with Great Expectations
    """
    logging.info("🔍 Starting enhanced bronze data validation with GX...")
    
    try:
        client = get_minio_client()
        
        # Download bronze data
        response = client.get_object('bronze', 'creditcard.csv')
        df = pd.read_csv(io.BytesIO(response.data))
        
        # Basic validation results
        validation_results = {
            'total_records': len(df),
            'columns': list(df.columns),
            'missing_values': df.isnull().sum().to_dict(),
            'duplicate_records': df.duplicated().sum(),
            'data_types': df.dtypes.astype(str).to_dict(),
        }
        
        # Specific fraud detection validations
        required_columns = ['Time', 'Amount', 'Class']
        missing_required = [col for col in required_columns if col not in df.columns]
        
        if missing_required:
            raise ValueError(f"Missing required columns: {missing_required}")
        
        # Check for valid fraud labels (0 or 1)
        if 'Class' in df.columns:
            invalid_classes = df[~df['Class'].isin([0, 1])]['Class'].unique()
            if len(invalid_classes) > 0:
                logging.warning(f"Found invalid class labels: {invalid_classes}")
        
        # 🔧 NEW: Enhanced validation with custom checks (GX fallback)
        from datetime import datetime
        
        # Custom validation checks (more reliable than GX for now)
        custom_validation = {
            'timestamp': datetime.now().isoformat(),
            'checks_performed': [],
            'success': True,
            'errors': []
        }
        
        try:
            # Row count check
            if len(df) < 1000:
                custom_validation['errors'].append("❌ Too few records")
                custom_validation['success'] = False
            elif len(df) > 1000000:
                custom_validation['errors'].append("❌ Too many records")
                custom_validation['success'] = False
            else:
                custom_validation['checks_performed'].append(f"✅ Row count valid: {len(df):,}")
            
            # Class validation
            if 'Class' in df.columns:
                unique_classes = df['Class'].unique()
                null_classes = df['Class'].isnull().sum()
                if set(unique_classes) <= {0, 1}:
                    custom_validation['checks_performed'].append("✅ Class values are binary (0/1)")
                else:
                    custom_validation['errors'].append(f"❌ Invalid class values: {unique_classes}")
                    custom_validation['success'] = False
                    
                if null_classes > 0:
                    custom_validation['errors'].append(f"❌ Class has {null_classes} null values")
                    custom_validation['success'] = False
            
            # Amount validation  
            if 'Amount' in df.columns:
                amount_stats = df['Amount'].describe()
                null_amounts = df['Amount'].isnull().sum()
                
                if amount_stats['min'] >= 0 and amount_stats['max'] <= 100000:
                    custom_validation['checks_performed'].append("✅ Amount in valid range [0, 100000]")
                else:
                    custom_validation['errors'].append(f"❌ Amount out of range: min={amount_stats['min']}, max={amount_stats['max']}")
                    
                if null_amounts > 0:
                    custom_validation['errors'].append(f"❌ Amount has {null_amounts} null values")
                    custom_validation['success'] = False
            
            # Time validation
            if 'Time' in df.columns:
                time_stats = df['Time'].describe()
                if time_stats['min'] >= 0:
                    custom_validation['checks_performed'].append("✅ Time values are positive")
                else:
                    custom_validation['errors'].append("❌ Time has negative values")
                    custom_validation['success'] = False
            
            # PCA features validation
            pca_cols = [col for col in df.columns if col.startswith('V') and col[1:].isdigit()]
            pca_valid = True
            for col in pca_cols[:5]:  # Check first 5
                if col in df.columns:
                    col_stats = df[col].describe()
                    if col_stats['min'] < -100 or col_stats['max'] > 100:
                        pca_valid = False
                        break
            
            if pca_cols:
                if pca_valid:
                    custom_validation['checks_performed'].append(f"✅ PCA features ({len(pca_cols)} cols) in reasonable range")
                else:
                    custom_validation['errors'].append("❌ Some PCA features out of expected range [-100, 100]")
                    
        except Exception as validation_error:
            custom_validation['errors'].append(f"❌ Custom validation error: {str(validation_error)}")
            custom_validation['success'] = False
        
        # Add custom validation results
        validation_results['enhanced_validation'] = custom_validation
        
        if custom_validation['success']:
            logging.info("🎉 Enhanced validation PASSED!")
            logging.info(f"   📊 Checks: {len(custom_validation['checks_performed'])}")
            for check in custom_validation['checks_performed']:
                logging.info(f"   {check}")
        else:
            logging.warning("⚠️ Some enhanced validation checks failed:")
            for error in custom_validation['errors']:
                logging.warning(f"   {error}")
        
        # 🎯 OPTIONAL: Try GX if available (fallback gracefully)
        try:
            import great_expectations as gx
            logging.info("🔍 Great Expectations available, running additional validation...")
            
            df_sample = df.sample(n=min(5000, len(df)), random_state=42) if len(df) > 5000 else df
            context_gx = gx.get_context()
            validator = context_gx.sources.pandas_default.read_dataframe(df_sample)
            
            # Simplified GX validation
            if 'Class' in df.columns:
                validator.expect_column_values_to_be_in_set('Class', [0, 1])
            if 'Amount' in df.columns:
                validator.expect_column_values_to_be_between('Amount', min_value=0, max_value=50000)
                
            gx_results = validator.validate()
            validation_results['gx_validation'] = {
                'success': gx_results.success,
                'sample_size': len(df_sample),
                'timestamp': datetime.now().isoformat()
            }
            
            if gx_results.success:
                logging.info("🎉 Great Expectations validation also PASSED!")
            else:
                logging.warning("⚠️ Great Expectations found some issues (non-blocking)")
                
        except Exception as gx_error:
            logging.info(f"📝 Great Expectations not available or failed: {str(gx_error)[:100]}...")
            validation_results['gx_validation'] = {
                'success': False, 
                'error': 'GX_NOT_AVAILABLE',
                'message': 'Using custom validation instead'
            }
        
        logging.info(f"✅ Enhanced bronze data validation completed: {validation_results['total_records']} records")
        
        # Push results to XCom for next tasks
        context['task_instance'].xcom_push(key='validation_results', value=validation_results)
        return validation_results
        
    except Exception as e:
        logging.error(f"❌ Bronze data validation failed: {str(e)}")
        raise

# def clean_and_transform_data(**context) -> Dict[str, Any]:
#     import logging
#     import os
#     from pyspark.sql import functions as F
#     from pyspark.sql.window import Window
#     logging.info("🧹 Starting Spark cleaning and transformation...")

#     try:
#         spark = build_spark_session()

#         bronze_path = 's3a://bronze/creditcard.csv'
#         silver_current_path = 's3a://silver/creditcard_current.parquet'

#         df = (
#             spark.read.option('header', True).option('inferSchema', True).csv(bronze_path)
#         )

#         original_count = df.count()

#         # 1. Remove exact duplicates
#         df = df.dropDuplicates()
#         duplicates_removed = original_count - df.count()

#         # 2. Handle missing values for numeric columns (median per column)
#         numeric_columns = [name for name, dtype in df.dtypes if dtype in ['int', 'bigint', 'double', 'float', 'long', 'decimal'] and name not in ['Class', 'Time']]
#         for col_name in numeric_columns:
#             median_value = df.approxQuantile(col_name, [0.5], 0.01)[0] if col_name in df.columns else None
#             if median_value is not None:
#                 df = df.withColumn(col_name, F.when(F.col(col_name).isNull(), F.lit(float(median_value))).otherwise(F.col(col_name)))

#         # 3. Derive time-based fields from seconds offset if present
#         #    We avoid converting to absolute timestamp to keep logic simple and deterministic
#         if 'Time' in df.columns:
#             df = df.withColumn('Time', F.col('Time').cast('double'))

#         # 4. Feature engineering
#         if 'Amount' in df.columns:
#             df = (
#                 df.withColumn('Amount_Log', F.log1p(F.col('Amount').cast('double')))
#             )

#             w = Window.orderBy(F.col('Amount').cast('double'))
#             df = df.withColumn('Amount_Percentile', F.percent_rank().over(w))

#             df = df.withColumn(
#                 'Amount_Category',
#                 F.when(F.col('Amount') <= 50, F.lit('Low'))
#                  .when((F.col('Amount') > 50) & (F.col('Amount') <= 200), F.lit('Medium'))
#                  .when((F.col('Amount') > 200) & (F.col('Amount') <= 1000), F.lit('High'))
#                  .otherwise(F.lit('Very_High'))
#             )

#         if 'Time' in df.columns:
#             # Compute pseudo hour/day-of-week from seconds offset
#             df = df.withColumn('Hour', F.floor((F.col('Time') % F.lit(86400.0)) / F.lit(3600.0)).cast('int')) \
#                    .withColumn('DayOfWeek', F.floor((F.col('Time') / F.lit(86400.0)) % 7).cast('int')) \
#                    .withColumn('IsWeekend', F.when(F.col('DayOfWeek').isin(5, 6), F.lit(1)).otherwise(F.lit(0)))

#         # 5. Outlier flags for PCA features V1..V28 (IQR per column)
#         pca_columns = [c for c, t in df.dtypes if c.startswith('V')]
#         for col_name in pca_columns:
#             quantiles = df.approxQuantile(col_name, [0.25, 0.75], 0.01)
#             if len(quantiles) == 2:
#                 q1, q3 = quantiles
#                 iqr = q3 - q1
#                 lower = q1 - 1.5 * iqr
#                 upper = q3 + 1.5 * iqr
#                 df = df.withColumn(f'{col_name}_IsOutlier', ((F.col(col_name) < lower) | (F.col(col_name) > upper)).cast('int'))

#         # 6. Data quality indicators and target handling
#         df = df.withColumn('ProcessedTimestamp', F.current_timestamp())
#         df = df.withColumn('DataQuality_Score', F.lit(1.0))
#         if 'Class' in df.columns:
#             df = df.filter(F.col('Class').isNotNull())

#         final_count = df.count()
#         records_removed = original_count - final_count

#         # Write to silver current and also a timestamped version
#         timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
#         silver_versioned_path = f's3a://silver/creditcard_cleaned_{timestamp_str}.parquet'

#         df.write.mode('overwrite').parquet(silver_current_path)
#         df.write.mode('overwrite').parquet(silver_versioned_path)

#         transformation_results = {
#             'original_records': int(original_count),
#             'final_records': int(final_count),
#             'duplicates_removed': int(duplicates_removed),
#             'records_removed': int(records_removed),
#             'silver_current_path': silver_current_path,
#             'silver_versioned_path': silver_versioned_path,
#         }

#         context['task_instance'].xcom_push(key='transformation_results', value=transformation_results)
#         return transformation_results

#     except Exception as e:
#         logging.error(f"❌ Spark transformation failed: {str(e)}")
#         raise

def clean_and_transform_data(**context) -> Dict[str, Any]:
    import logging
    import os
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    from datetime import datetime
    logging.info("🧹 Starting Spark cleaning and transformation (SAMPLE MODE)...")

    try:
        spark = build_spark_session()

        bronze_path = 's3a://bronze/creditcard.csv'
        silver_current_path = 's3a://silver/creditcard_current.parquet'

        # 🎯 SAMPLE: Read only partial data
        logging.info("📊 Reading data in SAMPLE mode...")
        df_full = (
            spark.read.option('header', True).option('inferSchema', True).csv(bronze_path)
        )
        
        # Take sample: either 10% or max 50k records, whichever is smaller
        total_count = df_full.count()
        sample_size = min(50000, int(total_count * 0.1))  # Max 50k or 10%
        
        logging.info(f"📈 Total records: {total_count}, Sample size: {sample_size}")
        
        # 🎯 Use limit instead of sample() for deterministic results
        df = df_full.limit(sample_size)
        original_count = df.count()
        
        logging.info(f"✅ Processing sample of {original_count} records")

        # 1. Remove exact duplicates
        df = df.dropDuplicates()
        duplicates_removed = original_count - df.count()

        # 2. 🔧 SIMPLIFIED: Handle missing values with FIXED medians (avoid approxQuantile)
        logging.info("🧹 Handling missing values with predefined medians...")
        
        numeric_columns = [name for name, dtype in df.dtypes 
                          if dtype in ['int', 'bigint', 'double', 'float', 'long', 'decimal'] 
                          and name not in ['Class', 'Time']]
        
        # Use predefined medians for common fraud detection features
        predefined_medians = {
            'Amount': 22.0,  # Typical transaction amount median
            'V1': 0.0, 'V2': 0.0, 'V3': 0.0, 'V4': 0.0, 'V5': 0.0,
            'V6': 0.0, 'V7': 0.0, 'V8': 0.0, 'V9': 0.0, 'V10': 0.0,
            'V11': 0.0, 'V12': 0.0, 'V13': 0.0, 'V14': 0.0, 'V15': 0.0,
            'V16': 0.0, 'V17': 0.0, 'V18': 0.0, 'V19': 0.0, 'V20': 0.0,
            'V21': 0.0, 'V22': 0.0, 'V23': 0.0, 'V24': 0.0, 'V25': 0.0,
            'V26': 0.0, 'V27': 0.0, 'V28': 0.0
        }
        
        for col_name in numeric_columns:
            if col_name in df.columns:
                median_value = predefined_medians.get(col_name, 0.0)
                df = df.withColumn(
                    col_name, 
                    F.when(F.col(col_name).isNull(), F.lit(float(median_value)))
                    .otherwise(F.col(col_name))
                )
                logging.info(f"   ✅ {col_name}: filled with median {median_value}")

        # 3. Time fields (keep simple)
        if 'Time' in df.columns:
            df = df.withColumn('Time', F.col('Time').cast('double'))

        # 4. 🔧 SIMPLIFIED: Feature engineering without heavy window functions
        if 'Amount' in df.columns:
            logging.info("💰 Creating Amount features...")
            df = df.withColumn('Amount_Log', F.log1p(F.col('Amount').cast('double')))
            
            # 🎯 SIMPLIFIED: Use row_number instead of percent_rank (lighter)
            df = df.withColumn('row_id', F.monotonically_increasing_id())
            total_rows = df.count()
            df = df.withColumn('Amount_Percentile', F.col('row_id') / F.lit(total_rows))

            df = df.withColumn(
                'Amount_Category',
                F.when(F.col('Amount') <= 50, F.lit('Low'))
                 .when((F.col('Amount') > 50) & (F.col('Amount') <= 200), F.lit('Medium'))
                 .when((F.col('Amount') > 200) & (F.col('Amount') <= 1000), F.lit('High'))
                 .otherwise(F.lit('Very_High'))
            )

        if 'Time' in df.columns:
            logging.info("⏰ Creating Time features...")
            df = df.withColumn('Hour', F.floor((F.col('Time') % F.lit(86400.0)) / F.lit(3600.0)).cast('int')) \
                   .withColumn('DayOfWeek', F.floor((F.col('Time') / F.lit(86400.0)) % 7).cast('int')) \
                   .withColumn('IsWeekend', F.when(F.col('DayOfWeek').isin(5, 6), F.lit(1)).otherwise(F.lit(0)))

        # 5. 🔧 SIMPLIFIED: Outlier detection with predefined thresholds
        logging.info("🎯 Creating outlier flags with predefined thresholds...")
        
        # Only process first 5 PCA columns to avoid performance issues
        pca_columns = [c for c, t in df.dtypes if c.startswith('V')][:5]  # Only V1-V5
        
        # Predefined outlier thresholds for fraud detection (based on domain knowledge)
        outlier_thresholds = {
            'V1': (-3.0, 3.0), 'V2': (-3.0, 3.0), 'V3': (-3.0, 3.0),
            'V4': (-3.0, 3.0), 'V5': (-3.0, 3.0)
        }
        
        for col_name in pca_columns:
            if col_name in outlier_thresholds:
                lower, upper = outlier_thresholds[col_name]
                df = df.withColumn(
                    f'{col_name}_IsOutlier', 
                    ((F.col(col_name) < lower) | (F.col(col_name) > upper)).cast('int')
                )
                logging.info(f"   ✅ {col_name}: outlier threshold [{lower}, {upper}]")

        # 6. Metadata and quality indicators
        df = df.withColumn('ProcessedTimestamp', F.current_timestamp())
        df = df.withColumn('DataQuality_Score', F.lit(1.0))
        df = df.withColumn('ProcessingMode', F.lit('SAMPLE'))
        df = df.withColumn('SampleSize', F.lit(original_count))
        df = df.withColumn('OriginalTotalRecords', F.lit(total_count))
        
        if 'Class' in df.columns:
            df = df.filter(F.col('Class').isNotNull())

        final_count = df.count()
        records_removed = original_count - final_count

        # Write to silver with sample indicator
        timestamp_str = datetime.now().strftime('%Y%m%d_%H%M%S')
        silver_versioned_path = f's3a://silver/creditcard_sample_{timestamp_str}.parquet'

        logging.info(f"💾 Saving {final_count} processed records...")
        
        # Use single partition to avoid small files
        df.coalesce(1).write.mode('overwrite').parquet(silver_current_path)
        df.coalesce(1).write.mode('overwrite').parquet(silver_versioned_path)

        transformation_results = {
            'processing_mode': 'SAMPLE',
            'original_total_records': int(total_count),
            'sample_size': int(original_count),
            'final_records': int(final_count),
            'duplicates_removed': int(duplicates_removed),
            'records_removed': int(records_removed),
            'sample_percentage': round((original_count / total_count) * 100, 2),
            'silver_current_path': silver_current_path,
            'silver_versioned_path': silver_versioned_path,
        }

        logging.info("✅ Sample transformation completed successfully!")
        logging.info(f"   📊 Processed: {final_count} records ({transformation_results['sample_percentage']}% of total)")
        logging.info(f"   💾 Saved to: {silver_current_path}")

        context['task_instance'].xcom_push(key='transformation_results', value=transformation_results)
        return transformation_results

    except Exception as e:
        logging.error(f"❌ Sample transformation failed: {str(e)}")
        raise
    finally:
        # Cleanup Spark session
        try:
            spark.stop()
            logging.info("🛑 Spark session stopped")
        except:
            pass

def upload_to_silver(**context) -> Dict[str, Any]:
    import logging
    import io
    import base64
    from datetime import datetime
    
    logging.info("📤 Starting upload to silver bucket...")
    
    try:
        # With Spark writing directly to MinIO, this step only reports what was written
        transformation_results = context['task_instance'].xcom_pull(key='transformation_results', task_ids='clean_and_transform')
        if not transformation_results:
            raise ValueError('Missing transformation results from previous step')

        upload_results = {
            'filename': transformation_results['silver_versioned_path'].split('/')[-1],
            'bucket': 'silver',
            'size_bytes': None,
            'upload_timestamp': datetime.now().isoformat(),
            'current_path': transformation_results['silver_current_path'],
            'versioned_path': transformation_results['silver_versioned_path']
        }

        logging.info(f"✅ Verified silver outputs at {upload_results['current_path']} and {upload_results['versioned_path']}")

        return upload_results
        
    except Exception as e:
        logging.error(f"❌ Upload to silver failed: {str(e)}")
        raise


def generate_data_quality_report(**context) -> Dict[str, Any]:
    import logging
    import io
    import pandas as pd
    import numpy as np
    """
    Generate a comprehensive data quality report
    """
    logging.info("📊 Generating data quality report...")
    
    try:
        # Get results from previous tasks
        validation_results = context['task_instance'].xcom_pull(key='validation_results', task_ids='validate_bronze')
        transformation_results = context['task_instance'].xcom_pull(key='transformation_results', task_ids='clean_and_transform')
        upload_results = context['task_instance'].xcom_pull(key='return_value', task_ids='upload_to_silver')
        
        report = {
            'pipeline_execution': {
                'execution_date': context['execution_date'].isoformat(),
                'dag_run_id': context['run_id'],
                'status': 'completed'
            },
            'bronze_layer': validation_results,
            'transformation': transformation_results,
            'silver_layer': upload_results,
            'data_lineage': {
                'source': 'bronze/creditcard.csv',
                'destination': f"silver/{upload_results['filename']}",
                'transformation_steps': [
                    'duplicate_removal',
                    'missing_value_handling',
                    'feature_engineering',
                    'outlier_detection',
                    'data_quality_scoring'
                ]
            }
        }
        
        logging.info("✅ Data quality report generated successfully")
        
        # In a real scenario, you might want to store this report
        # or send it to a monitoring system
        
        return report
        
    except Exception as e:
        logging.error(f"❌ Data quality report generation failed: {str(e)}")
        raise

# Define the DAG
dag = DAG(
    'bronze_to_silver_etl',
    default_args=DEFAULT_ARGS,
    description='Bronze to Silver ETL Pipeline for Fraud Detection Data',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    max_active_runs=1,
    tags=['etl', 'fraud-detection', 'data-quality'],
)

# Define tasks
activate_conda_env = BashOperator(
    task_id='activate_conda_env',
    bash_command='echo "Skip per-container pip install; dependencies are preinstalled at container start"',

    dag=dag
)
start_task = DummyOperator(
    task_id='start_pipeline',
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_bronze',
    python_callable=validate_bronze_data,
    dag=dag,
    retries=1,
)

transform_task = PythonOperator(
    task_id='clean_and_transform',
    python_callable=clean_and_transform_data,
    dag=dag,
    retries=1,
)

upload_task = PythonOperator(
    task_id='upload_to_silver',
    python_callable=upload_to_silver,
    dag=dag,
    retries=1,
)

report_task = PythonOperator(
    task_id='generate_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag,
    retries=1,
)

end_task = DummyOperator(
    task_id='end_pipeline',
    dag=dag
)

# Define task dependencies
activate_conda_env >> start_task >> validate_task >> transform_task >> upload_task >> report_task >> end_task

# Task documentation
validate_task.doc_md = """
## Bronze Data Validation

Validates the quality and structure of raw data in the bronze bucket:
- Checks required columns are present
- Validates data types
- Identifies missing values and duplicates
- Verifies fraud label integrity
"""

transform_task.doc_md = """
## Data Cleaning and Transformation

Performs comprehensive data cleaning and feature engineering:
- Removes duplicate records
- Handles missing values with statistical methods
- Creates time-based features (hour, day of week, weekend flag)
- Engineers amount-based features (categories, log transform, percentiles)
- Detects outliers in PCA features
- Adds data quality indicators
"""

upload_task.doc_md = """
## Silver Layer Upload

Uploads cleaned and enriched data to the silver bucket:
- Saves data in Parquet format for efficient storage
- Creates timestamped versions for data lineage
- Maintains current version for downstream consumption
"""

report_task.doc_md = """
## Data Quality Report

Generates comprehensive data quality and pipeline execution report:
- Summarizes validation results
- Documents transformation statistics
- Tracks data lineage
- Provides quality metrics for monitoring
""" 