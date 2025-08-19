#!/usr/bin/env python3
"""
Gold Layer Data Checker for Fraud Detection Project
Validates and inspects gold layer datasets in MinIO
"""

import pandas as pd
import numpy as np
from minio import Minio
import io
import json
from datetime import datetime
import sys


def get_minio_client():
    """Initialize MinIO client"""
    return Minio(
        'localhost:9000',
        access_key='minioadmin',
        secret_key='minioadmin',
        secure=False
    )

def check_gold_bucket_exists():
    """Check if gold bucket exists"""
    try:
        client = get_minio_client()
        if client.bucket_exists('gold'):
            print("✅ Gold bucket exists")
            return True
        else:
            print("❌ Gold bucket does not exist")
            return False
    except Exception as e:
        print(f"❌ Error checking gold bucket: {e}")
        return False

def list_gold_datasets():
    """List all datasets in gold bucket"""
    try:
        client = get_minio_client()
        
        if not client.bucket_exists('gold'):
            print("❌ Gold bucket does not exist")
            return []
        
        datasets = []
        print("\n📂 Gold Layer Datasets:")
        print("=" * 50)
        
        for obj in client.list_objects('gold'):
            size_mb = obj.size / (1024 * 1024)
            datasets.append({
                'name': obj.object_name,
                'size_mb': size_mb,
                'last_modified': obj.last_modified
            })
            print(f"📄 {obj.object_name}")
            print(f"   Size: {size_mb:.2f} MB")
            print(f"   Modified: {obj.last_modified}")
            print()
        
        return datasets
        
    except Exception as e:
        print(f"❌ Error listing gold datasets: {e}")
        return []

def analyze_dataset(dataset_name):
    """Analyze a specific gold dataset"""
    try:
        client = get_minio_client()
        
        print(f"\n🔍 Analyzing Dataset: {dataset_name}")
        print("=" * 50)
        
        # Download and load dataset
        response = client.get_object('gold', dataset_name)
        df = pd.read_parquet(io.BytesIO(response.data))
        
        # Basic info
        print(f"📊 Dataset Shape: {df.shape}")
        print(f"📋 Columns ({len(df.columns)}): {list(df.columns)}")
        print(f"💾 Memory Usage: {df.memory_usage(deep=True).sum() / 1024 / 1024:.2f} MB")
        
        # Data types
        print(f"\n📋 Data Types:")
        for col, dtype in df.dtypes.items():
            print(f"   {col}: {dtype}")
        
        # Missing values
        print(f"\n❓ Missing Values:")
        missing = df.isnull().sum()
        if missing.sum() > 0:
            for col, count in missing[missing > 0].items():
                print(f"   {col}: {count} ({count/len(df)*100:.1f}%)")
        else:
            print("   No missing values ✅")
        
        # Basic statistics for numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        if len(numeric_cols) > 0:
            print(f"\n📈 Numeric Summary:")
            print(df[numeric_cols].describe())
        
        # Value counts for categorical columns
        categorical_cols = df.select_dtypes(include=['object']).columns
        if len(categorical_cols) > 0:
            print(f"\n🏷️ Categorical Summary:")
            for col in categorical_cols[:5]:  # Show first 5 categorical columns
                print(f"\n   {col} (unique values: {df[col].nunique()}):")
                print(f"   {df[col].value_counts().head()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error analyzing dataset {dataset_name}: {e}")
        return None

def validate_fraud_analytics():
    """Validate fraud analytics dataset"""
    try:
        print(f"\n🔍 Validating Fraud Analytics Dataset")
        print("=" * 50)
        
        client = get_minio_client()
        response = client.get_object('gold', 'fraud_analytics_hourly.parquet')
        df = pd.read_parquet(io.BytesIO(response.data))
        
        # Expected columns
        expected_cols = [
            'timestamp', 'hour', 'is_weekend', 'fraud_count', 'total_transactions',
            'total_amount', 'avg_amount', 'median_amount', 'std_amount',
            'avg_data_quality', 'amount_category_dist', 'fraud_rate',
            'created_at', 'data_version'
        ]
        
        # Check columns
        missing_cols = [col for col in expected_cols if col not in df.columns]
        extra_cols = [col for col in df.columns if col not in expected_cols]
        
        if missing_cols:
            print(f"❌ Missing columns: {missing_cols}")
        else:
            print("✅ All expected columns present")
        
        if extra_cols:
            print(f"ℹ️ Extra columns: {extra_cols}")
        
        # Validate data
        print(f"\n📊 Data Validation:")
        print(f"   Total records: {len(df)}")
        print(f"   Hour range: {df['hour'].min()} - {df['hour'].max()}")
        print(f"   Fraud rate range: {df['fraud_rate'].min():.4f} - {df['fraud_rate'].max():.4f}")
        print(f"   Total transactions: {df['total_transactions'].sum():,}")
        print(f"   Total fraud cases: {df['fraud_count'].sum():,}")
        print(f"   Overall fraud rate: {df['fraud_count'].sum() / df['total_transactions'].sum():.4f}")
        
        # Time range
        if 'timestamp' in df.columns:
            print(f"   Time range: {df['timestamp'].min()} to {df['timestamp'].max()}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error validating fraud analytics: {e}")
        return None

def validate_ml_features():
    """Validate ML features dataset"""
    try:
        print(f"\n🔍 Validating ML Features Dataset")
        print("=" * 50)
        
        client = get_minio_client()
        
        # Check for ML features files
        datasets = ['ml_features_current.parquet', 'ml_features_train.parquet', 'ml_features_test.parquet']
        
        for dataset in datasets:
            try:
                response = client.get_object('gold', dataset)
                df = pd.read_parquet(io.BytesIO(response.data))
                
                print(f"\n📄 {dataset}:")
                print(f"   Shape: {df.shape}")
                print(f"   Features: {len(df.columns)}")
                
                if 'Class' in df.columns:
                    fraud_rate = df['Class'].mean()
                    print(f"   Fraud rate: {fraud_rate:.4f}")
                    print(f"   Fraud cases: {df['Class'].sum():,}")
                    print(f"   Normal cases: {(df['Class'] == 0).sum():,}")
                
                # Check for engineered features
                interaction_features = [col for col in df.columns if 'Interaction' in col]
                sin_cos_features = [col for col in df.columns if col.endswith('_Sin') or col.endswith('_Cos')]
                
                print(f"   Interaction features: {len(interaction_features)}")
                print(f"   Sin/Cos features: {len(sin_cos_features)}")
                
            except Exception as e:
                print(f"❌ {dataset} not found or error: {e}")
        
    except Exception as e:
        print(f"❌ Error validating ML features: {e}")

def validate_risk_profiles():
    """Validate risk profiles dataset"""
    try:
        print(f"\n🔍 Validating Risk Profiles Dataset")
        print("=" * 50)
        
        client = get_minio_client()
        response = client.get_object('gold', 'risk_profiles_current.parquet')
        df = pd.read_parquet(io.BytesIO(response.data))
        
        print(f"📊 Risk Profiles Summary:")
        print(f"   Total segments: {len(df)}")
        print(f"   Risk categories: {df['risk_category'].value_counts().to_dict()}")
        print(f"   Risk score range: {df['risk_score'].min():.4f} - {df['risk_score'].max():.4f}")
        
        # Show top risk segments
        print(f"\n🚨 Top 5 Highest Risk Segments:")
        top_risk = df.nlargest(5, 'risk_score')[['customer_segment', 'risk_score', 'risk_category', 'fraud_rate']]
        print(top_risk.to_string(index=False))
        
        return df
        
    except Exception as e:
        print(f"❌ Error validating risk profiles: {e}")
        return None

def validate_summary_stats():
    """Validate summary statistics dataset"""
    try:
        print(f"\n🔍 Validating Summary Statistics Dataset")
        print("=" * 50)
        
        client = get_minio_client()
        response = client.get_object('gold', 'summary_stats_current.parquet')
        df = pd.read_parquet(io.BytesIO(response.data))
        
        print(f"📊 Summary Statistics:")
        print(f"   Total records: {len(df)}")
        print(f"   Aggregation levels: {df['aggregation_level'].value_counts().to_dict()}")
        
        # Show daily stats
        daily_stats = df[df['aggregation_level'] == 'daily']
        if len(daily_stats) > 0:
            print(f"\n📅 Daily Statistics:")
            print(f"   Date range: {daily_stats['date'].min()} to {daily_stats['date'].max()}")
            print(f"   Total daily records: {len(daily_stats)}")
            print(f"   Avg daily fraud rate: {daily_stats['fraud_rate'].mean():.4f}")
        
        # Show weekly stats
        weekly_stats = df[df['aggregation_level'] == 'weekly']
        if len(weekly_stats) > 0:
            print(f"\n📊 Weekly Statistics:")
            print(f"   Total weekly records: {len(weekly_stats)}")
            print(f"   Avg weekly fraud rate: {weekly_stats['fraud_rate'].mean():.4f}")
        
        return df
        
    except Exception as e:
        print(f"❌ Error validating summary stats: {e}")
        return None

def generate_gold_report():
    """Generate comprehensive gold layer report"""
    try:
        print(f"\n📋 Gold Layer Data Quality Report")
        print("=" * 70)
        print(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check bucket
        if not check_gold_bucket_exists():
            return
        
        # List datasets
        datasets = list_gold_datasets()
        
        if not datasets:
            print("❌ No datasets found in gold bucket")
            return
        
        # Validate each dataset type
        validate_fraud_analytics()
        validate_ml_features()
        validate_risk_profiles()
        validate_summary_stats()
        
        # Overall summary
        print(f"\n📊 Overall Gold Layer Summary")
        print("=" * 50)
        print(f"   Total datasets: {len(datasets)}")
        print(f"   Total size: {sum(d['size_mb'] for d in datasets):.2f} MB")
        
        # Data freshness
        latest_dataset = max(datasets, key=lambda x: x['last_modified'])
        print(f"   Latest dataset: {latest_dataset['name']}")
        print(f"   Last updated: {latest_dataset['last_modified']}")
        
        print(f"\n✅ Gold layer data quality check completed!")
        
    except Exception as e:
        print(f"❌ Error generating gold report: {e}")

def main():
    """Main function"""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == 'list':
            list_gold_datasets()
        elif command == 'analyze' and len(sys.argv) > 2:
            dataset_name = sys.argv[2]
            analyze_dataset(dataset_name)
        elif command == 'validate':
            if len(sys.argv) > 2:
                dataset_type = sys.argv[2]
                if dataset_type == 'fraud':
                    validate_fraud_analytics()
                elif dataset_type == 'ml':
                    validate_ml_features()
                elif dataset_type == 'risk':
                    validate_risk_profiles()
                elif dataset_type == 'summary':
                    validate_summary_stats()
                else:
                    print("Available validation types: fraud, ml, risk, summary")
            else:
                print("Usage: python check_data_gold.py validate <type>")
        elif command == 'report':
            generate_gold_report()
        else:
            print("Usage: python check_data_gold.py [list|analyze <dataset>|validate <type>|report]")
    else:
        # Default: show full report
        generate_gold_report()

if __name__ == "__main__":
    main()