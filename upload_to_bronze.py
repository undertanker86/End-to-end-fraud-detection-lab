#!/usr/bin/env python3
"""
Script to upload creditcard.csv to MinIO bronze bucket
"""

import os
from minio import Minio
from minio.error import S3Error
import sys

def upload_to_bronze():
    # MinIO configuration from docker-compose
    MINIO_URL = "localhost:9000"
    ACCESS_KEY = "minioadmin"
    SECRET_KEY = "minioadmin"
    BUCKET_NAME = "bronze"
    
    # File paths
    SOURCE_FILE = "/home/always/FSDS-Lab/Fraud-Detection-Project/raw-data/archive/creditcard.csv"
    OBJECT_NAME = "creditcard.csv"
    
    try:
        # Initialize MinIO client
        print("Connecting to MinIO...")
        client = Minio(
            MINIO_URL,
            access_key=ACCESS_KEY,
            secret_key=SECRET_KEY,
            secure=False  # Set to False for HTTP
        )
        
        # Check if bucket exists, create if it doesn't
        print(f"Checking if bucket '{BUCKET_NAME}' exists...")
        if not client.bucket_exists(BUCKET_NAME):
            print(f"Bucket '{BUCKET_NAME}' does not exist. Creating it...")
            client.make_bucket(BUCKET_NAME)
            print(f"Bucket '{BUCKET_NAME}' created successfully!")
        else:
            print(f"Bucket '{BUCKET_NAME}' already exists.")
        
        # Check if source file exists
        if not os.path.exists(SOURCE_FILE):
            print(f"Error: Source file '{SOURCE_FILE}' not found!")
            return False
            
        # Get file size for progress tracking
        file_size = os.path.getsize(SOURCE_FILE)
        print(f"File size: {file_size / (1024*1024):.2f} MB")
        
        # Upload file
        print(f"Uploading '{SOURCE_FILE}' to bucket '{BUCKET_NAME}'...")
        print("This may take a few moments for large files...")
        
        client.fput_object(
            BUCKET_NAME,
            OBJECT_NAME,
            SOURCE_FILE
        )
        
        print(f"\n✅ Successfully uploaded '{OBJECT_NAME}' to bucket '{BUCKET_NAME}'!")
        
        # Verify upload by getting object info
        obj_info = client.stat_object(BUCKET_NAME, OBJECT_NAME)
        print(f"📊 Object info:")
        print(f"   - Size: {obj_info.size / (1024*1024):.2f} MB")
        print(f"   - Last modified: {obj_info.last_modified}")
        print(f"   - ETag: {obj_info.etag}")
        
        return True
        
    except S3Error as e:
        print(f"❌ S3 Error occurred: {e}")
        return False
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return False

if __name__ == "__main__":
    print("=" * 50)
    print("MinIO Bronze Bucket Upload Script")
    print("=" * 50)
    
    success = upload_to_bronze()
    
    if success:
        print("\n🎉 Upload completed successfully!")
        sys.exit(0)
    else:
        print("\n💥 Upload failed!")
        sys.exit(1) 