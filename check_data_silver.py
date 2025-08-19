#!/usr/bin/env python3
"""
Script to validate and analyze data in the silver bucket after ETL pipeline execution
"""

import pandas as pd
import numpy as np
import os
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import io

def get_minio_client():
    """Initialize MinIO client"""
    MINIO_ENDPOINT = 'localhost:9000'  # Direct host connection
    MINIO_ACCESS_KEY = 'minioadmin'
    MINIO_SECRET_KEY = 'minioadmin'
    
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )

def check_silver_bucket():
    """Check what files exist in silver bucket"""
    print("🔍 Checking silver bucket contents...")
    
    try:
        client = get_minio_client()
        
        # List all objects in silver bucket
        objects = client.list_objects('silver', recursive=True)
        files = []
        
        for obj in objects:
            file_info = {
                'name': obj.object_name,
                'size': obj.size,
                'last_modified': obj.last_modified,
                'etag': obj.etag
            }
            files.append(file_info)
            print(f"📁 {obj.object_name} - {obj.size} bytes - {obj.last_modified}")
        
        return files
        
    except S3Error as e:
        print(f"❌ Error accessing silver bucket: {e}")
        return []

def analyze_current_parquet():
    """Analyze the current parquet file in silver bucket"""
    print("\n📊 Analyzing creditcard_current.parquet...")
    
    try:
        client = get_minio_client()
        
        # Download the current parquet file
        response = client.get_object('silver', 'creditcard_current.parquet')
        df = pd.read_parquet(io.BytesIO(response.data))
        
        print(f"✅ Successfully loaded parquet file")
        print(f"📏 Shape: {df.shape}")
        print(f"🗂️  Columns ({len(df.columns)}): {list(df.columns)}")
        
        # Basic statistics
        print("\n📈 Basic Statistics:")
        print(f"   • Total records: {len(df):,}")
        print(f"   • Memory usage: {df.memory_usage(deep=True).sum() / 1024**2:.2f} MB")
        
        # Check for new features
        new_features = [
            'Amount_Category', 'Amount_Log', 'Amount_Percentile',
            'Hour', 'DayOfWeek', 'IsWeekend', 'TimeOfDay',
            'ProcessedTimestamp', 'DataQuality_Score'
        ]
        
        print("\n🔧 Feature Engineering Check:")
        for feature in new_features:
            if feature in df.columns:
                print(f"   ✅ {feature} - Present")
            else:
                print(f"   ❌ {feature} - Missing")
        
        # Data quality checks
        print("\n🔍 Data Quality Analysis:")
        print(f"   • Missing values: {df.isnull().sum().sum()}")
        print(f"   • Duplicate records: {df.duplicated().sum()}")
        
        if 'Class' in df.columns:
            fraud_dist = df['Class'].value_counts()
            print(f"   • Fraud distribution:")
            print(f"     - Normal (0): {fraud_dist.get(0, 0):,} ({fraud_dist.get(0, 0)/len(df)*100:.2f}%)")
            print(f"     - Fraud (1): {fraud_dist.get(1, 0):,} ({fraud_dist.get(1, 0)/len(df)*100:.2f}%)")
        
        # Amount analysis
        if 'Amount' in df.columns:
            print(f"\n💰 Amount Analysis:")
            print(f"   • Min: ${df['Amount'].min():.2f}")
            print(f"   • Max: ${df['Amount'].max():.2f}")
            print(f"   • Mean: ${df['Amount'].mean():.2f}")
            print(f"   • Median: ${df['Amount'].median():.2f}")
            
            if 'Amount_Category' in df.columns:
                print(f"   • Amount categories:")
                category_dist = df['Amount_Category'].value_counts()
                for cat, count in category_dist.items():
                    print(f"     - {cat}: {count:,} ({count/len(df)*100:.1f}%)")
        
        # Time analysis
        if 'Hour' in df.columns:
            print(f"\n🕐 Time Analysis:")
            print(f"   • Hour range: {df['Hour'].min()} - {df['Hour'].max()}")
            
            if 'IsWeekend' in df.columns:
                weekend_dist = df['IsWeekend'].value_counts()
                print(f"   • Weekend distribution:")
                print(f"     - Weekday: {weekend_dist.get(0, 0):,} ({weekend_dist.get(0, 0)/len(df)*100:.1f}%)")
                print(f"     - Weekend: {weekend_dist.get(1, 0):,} ({weekend_dist.get(1, 0)/len(df)*100:.1f}%)")
        
        # PCA features with outliers
        pca_columns = [col for col in df.columns if col.startswith('V')]
        outlier_columns = [col for col in df.columns if col.endswith('_IsOutlier')]
        
        if outlier_columns:
            print(f"\n🎯 Outlier Analysis:")
            print(f"   • PCA features analyzed: {len(pca_columns)}")
            print(f"   • Outlier flags created: {len(outlier_columns)}")
            
            total_outliers = df[outlier_columns].sum().sum()
            print(f"   • Total outlier flags: {total_outliers:,}")
        
        # Processing metadata
        if 'ProcessedTimestamp' in df.columns:
            print(f"\n⏰ Processing Info:")
            print(f"   • Last processed: {df['ProcessedTimestamp'].iloc[0]}")
            
        if 'DataQuality_Score' in df.columns:
            print(f"   • Data quality score: {df['DataQuality_Score'].mean():.2f}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error analyzing parquet file: {e}")
        return None

def compare_with_bronze():
    """Compare silver data with original bronze data"""
    print("\n🔄 Comparing with bronze data...")
    
    try:
        client = get_minio_client()
        
        # Load bronze data
        bronze_response = client.get_object('bronze', 'creditcard.csv')
        bronze_df = pd.read_csv(io.BytesIO(bronze_response.data))
        
        # Load silver data
        silver_response = client.get_object('silver', 'creditcard_current.parquet')
        silver_df = pd.read_parquet(io.BytesIO(silver_response.data))
        
        print(f"📊 Record count comparison:")
        print(f"   • Bronze: {len(bronze_df):,} records")
        print(f"   • Silver: {len(silver_df):,} records")
        print(f"   • Difference: {len(bronze_df) - len(silver_df):,} records removed")
        
        print(f"\n🗂️  Column comparison:")
        print(f"   • Bronze columns: {len(bronze_df.columns)}")
        print(f"   • Silver columns: {len(silver_df.columns)}")
        print(f"   • New columns added: {len(silver_df.columns) - len(bronze_df.columns)}")
        
        # New columns
        new_columns = set(silver_df.columns) - set(bronze_df.columns)
        if new_columns:
            print(f"   • Added columns: {sorted(list(new_columns))}")
        
        return True
        
    except Exception as e:
        print(f"❌ Error comparing with bronze: {e}")
        return False

def check_file_system():
    """Check if file exists in local file system"""
    file_path = '/home/always/FSDS-Lab/Fraud-Detection-Project/minio_data/silver/creditcard_current.parquet'
    
    print(f"\n📁 Checking local file system...")
    
    if os.path.exists(file_path):
        file_size = os.path.getsize(file_path)
        file_mtime = datetime.fromtimestamp(os.path.getmtime(file_path))
        
        print(f"✅ File exists: {file_path}")
        print(f"   • Size: {file_size:,} bytes ({file_size/1024**2:.2f} MB)")
        print(f"   • Last modified: {file_mtime}")
        
        # Try to read directly from file system
        try:
            df = pd.read_parquet(file_path)
            print(f"   • Readable: Yes ({df.shape[0]:,} rows, {df.shape[1]} columns)")
            return True
        except Exception as e:
            print(f"   • Readable: No - {e}")
            return False
    else:
        print(f"❌ File not found: {file_path}")
        return False

def main():
    """Main function to run all checks"""
    print("🚀 Starting Silver Data Validation")
    print("=" * 60)
    
    # Check MinIO bucket
    files = check_silver_bucket()
    
    if not files:
        print("❌ No files found in silver bucket or bucket doesn't exist")
        return
    
    # Analyze current parquet file
    df = analyze_current_parquet()
    print(df.head() if df is not None else "No data to display")
    # Save csv file
    if df is not None:
        csv_path = '/home/always/FSDS-Lab/Fraud-Detection-Project/raw-data/archive/creditcard_current.csv'
        df.to_csv(csv_path, index=False)
        print(f"✅ Saved current data to {csv_path}")
    
    if df is not None:
        # Compare with bronze
        compare_with_bronze()
        
        # Check file system
        check_file_system()
        
        print("\n" + "=" * 60)
        print("✅ Silver data validation completed successfully!")
    else:
        print("\n❌ Silver data validation failed!")

if __name__ == "__main__":
    main()