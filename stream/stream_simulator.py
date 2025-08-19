import pandas as pd
import psycopg2
import time
import random
import logging
from datetime import datetime
from typing import Optional
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FraudDataStreamSimulator:
    def __init__(self, 
                 csv_file_path: str,
                 db_config: dict,
                 batch_size: int = 1,
                 delay_range: tuple = (1, 5)):
        """
        Initialize the stream simulator
        
        Args:
            csv_file_path: Path to the fake-stream-data.csv file
            db_config: Database connection configuration
            batch_size: Number of records to insert at once
            delay_range: Min and max delay between batches in seconds
        """
        self.csv_file_path = csv_file_path
        self.db_config = db_config
        self.batch_size = batch_size
        self.delay_range = delay_range
        self.connection: Optional[psycopg2.connection] = None
        
    def connect_to_db(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = True
            logger.info("✅ Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise
    
    def disconnect_from_db(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Disconnected from database")
    
    def create_table_if_not_exists(self):
        """Create fraud_transactions table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fraud_transactions (
            id SERIAL PRIMARY KEY,
            time_stamp BIGINT NOT NULL,
            v1 DECIMAL(20,10), v2 DECIMAL(20,10), v3 DECIMAL(20,10), v4 DECIMAL(20,10), v5 DECIMAL(20,10),
            v6 DECIMAL(20,10), v7 DECIMAL(20,10), v8 DECIMAL(20,10), v9 DECIMAL(20,10), v10 DECIMAL(20,10),
            v11 DECIMAL(20,10), v12 DECIMAL(20,10), v13 DECIMAL(20,10), v14 DECIMAL(20,10), v15 DECIMAL(20,10),
            v16 DECIMAL(20,10), v17 DECIMAL(20,10), v18 DECIMAL(20,10), v19 DECIMAL(20,10), v20 DECIMAL(20,10),
            v21 DECIMAL(20,10), v22 DECIMAL(20,10), v23 DECIMAL(20,10), v24 DECIMAL(20,10), v25 DECIMAL(20,10),
            v26 DECIMAL(20,10), v27 DECIMAL(20,10), v28 DECIMAL(20,10),
            amount DECIMAL(10,2) NOT NULL,
            class INTEGER NOT NULL CHECK (class IN (0, 1)),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_time ON fraud_transactions(time_stamp);
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_class ON fraud_transactions(class);
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_amount ON fraud_transactions(amount);
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_created_at ON fraud_transactions(created_at);
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            logger.info("✅ Table fraud_transactions created/verified")
        except Exception as e:
            logger.error(f"❌ Failed to create table: {e}")
            raise
    
    def load_csv_data(self) -> pd.DataFrame:
        """Load and prepare CSV data"""
        try:
            # Read CSV file
            df = pd.read_csv(self.csv_file_path)
            
            # Rename columns to match our table structure
            df.columns = ['time_stamp'] + [f'v{i}' for i in range(1, 29)] + ['amount', 'class']
            
            # Shuffle data to simulate random order
            df = df.sample(frac=1).reset_index(drop=True)
            
            logger.info(f"✅ Loaded {len(df)} records from CSV")
            logger.info(f"📊 Fraud distribution: {df['class'].value_counts().to_dict()}")
            
            return df
        except Exception as e:
            logger.error(f"❌ Failed to load CSV data: {e}")
            raise
    
    def insert_batch(self, batch_data: pd.DataFrame):
        """Insert a batch of records into the database"""
        insert_sql = """
        INSERT INTO fraud_transactions (
            time_stamp, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15,
            v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, amount, class
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare batch data
            batch_values = []
            for _, row in batch_data.iterrows():
                values = tuple(row.values)
                batch_values.append(values)
            
            # Execute batch insert
            cursor.executemany(insert_sql, batch_values)
            
            fraud_count = batch_data[batch_data['class'] == 1].shape[0]
            normal_count = batch_data[batch_data['class'] == 0].shape[0]
            
            logger.info(f"📥 Inserted batch of {len(batch_data)} records "
                       f"(🚨 {fraud_count} fraud, ✅ {normal_count} normal)")
            
        except Exception as e:
            logger.error(f"❌ Failed to insert batch: {e}")
            raise
    
    def simulate_stream(self, total_records: Optional[int] = None):
        """Simulate streaming data insertion"""
        try:
            # Connect to database
            self.connect_to_db()
            
            # Create table if needed
            self.create_table_if_not_exists()
            
            # Load CSV data
            df = self.load_csv_data()
            
            # Limit records if specified
            if total_records:
                df = df.head(total_records)
                logger.info(f"🎯 Limited to {total_records} records for simulation")
            
            total_batches = len(df) // self.batch_size + (1 if len(df) % self.batch_size > 0 else 0)
            
            logger.info(f"🚀 Starting stream simulation:")
            logger.info(f"   📋 Total records: {len(df)}")
            logger.info(f"   📦 Batch size: {self.batch_size}")
            logger.info(f"   🔄 Total batches: {total_batches}")
            logger.info(f"   ⏱️  Delay range: {self.delay_range[0]}-{self.delay_range[1]} seconds")
            
            # Process in batches
            for i in range(0, len(df), self.batch_size):
                batch_end = min(i + self.batch_size, len(df))
                batch_data = df.iloc[i:batch_end]
                
                # Insert batch
                self.insert_batch(batch_data)
                
                # Progress tracking
                batch_num = (i // self.batch_size) + 1
                progress = (batch_num / total_batches) * 100
                logger.info(f"📊 Progress: {batch_num}/{total_batches} batches ({progress:.1f}%)")
                
                # Random delay between batches (except for last batch)
                if batch_end < len(df):
                    delay = random.uniform(self.delay_range[0], self.delay_range[1])
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before next batch...")
                    time.sleep(delay)
            
            logger.info("🎉 Stream simulation completed successfully!")
            
        except KeyboardInterrupt:
            logger.info("⏹️  Stream simulation interrupted by user")
        except Exception as e:
            logger.error(f"❌ Stream simulation failed: {e}")
            raise
        finally:
            self.disconnect_from_db()
    
    def get_table_stats(self):
        """Get current table statistics"""
        try:
            self.connect_to_db()
            cursor = self.connection.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM fraud_transactions")
            total_count = cursor.fetchone()[0]
            
            # Get fraud distribution
            cursor.execute("SELECT class, COUNT(*) FROM fraud_transactions GROUP BY class ORDER BY class")
            distribution = cursor.fetchall()
            
            # Get recent records
            cursor.execute("""
                SELECT COUNT(*) FROM fraud_transactions 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
            """)
            recent_count = cursor.fetchone()[0]
            
            logger.info(f"📊 Table Statistics:")
            logger.info(f"   📋 Total records: {total_count}")
            logger.info(f"   🕐 Records in last hour: {recent_count}")
            logger.info(f"   📈 Distribution:")
            for class_val, count in distribution:
                class_name = "Normal" if class_val == 0 else "Fraud"
                percentage = (count / total_count * 100) if total_count > 0 else 0
                logger.info(f"      {class_name}: {count} ({percentage:.2f}%)")
            
        except Exception as e:
            logger.error(f"❌ Failed to get table stats: {e}")
        finally:
            self.disconnect_from_db()

def main():
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5434,
        'database': 'fraud_detection',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    # CSV file path
    csv_file_path = '/home/always/FSDS-Lab/Fraud-Detection-Project/raw-data/archive/fake-stream-data.csv'
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        logger.error(f"❌ CSV file not found: {csv_file_path}")
        sys.exit(1)
    
    # Create simulator
    simulator = FraudDataStreamSimulator(
        csv_file_path=csv_file_path,
        db_config=db_config,
        batch_size=5,  # Insert 5 records at a time
        delay_range=(2, 8)  # Wait 2-8 seconds between batches
    )
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == 'stats':
            simulator.get_table_stats()
            return
        elif sys.argv[1] == 'simulate':
            records_limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
            simulator.simulate_stream(total_records=records_limit)
            return
    
    # Default: run simulation
    logger.info("🎮 Fraud Detection Stream Simulator")
    logger.info("Usage:")
    logger.info("  python stream_simulator.py simulate [limit]  # Run simulation")
    logger.info("  python stream_simulator.py stats            # Show table stats")
    logger.info("")
    
    # Ask user what to do
    choice = input("Choose action (1=simulate, 2=stats, 3=simulate with limit): ").strip()
    
    if choice == '1':
        simulator.simulate_stream()
    elif choice == '2':
        simulator.get_table_stats()
    elif choice == '3':
        limit = int(input("Enter record limit: "))
        simulator.simulate_stream(total_records=limit)
    else:
        logger.info("Invalid choice. Running default simulation...")
        simulator.simulate_stream(total_records=100)  # Default to 100 records

if __name__ == "__main__":
    main()
import pandas as pd
import psycopg2
import time
import random
import logging
from datetime import datetime
from typing import Optional
import sys
import os

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class FraudDataStreamSimulator:
    def __init__(self, 
                 csv_file_path: str,
                 db_config: dict,
                 batch_size: int = 1,
                 delay_range: tuple = (1, 5)):
        """
        Initialize the stream simulator
        
        Args:
            csv_file_path: Path to the fake-stream-data.csv file
            db_config: Database connection configuration
            batch_size: Number of records to insert at once
            delay_range: Min and max delay between batches in seconds
        """
        self.csv_file_path = csv_file_path
        self.db_config = db_config
        self.batch_size = batch_size
        self.delay_range = delay_range
        self.connection: Optional[psycopg2.connection] = None
        
    def connect_to_db(self):
        """Establish database connection"""
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.connection.autocommit = True
            logger.info("✅ Connected to PostgreSQL database")
        except Exception as e:
            logger.error(f"❌ Failed to connect to database: {e}")
            raise
    
    def disconnect_from_db(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("🔌 Disconnected from database")
    
    def create_table_if_not_exists(self):
        """Create fraud_transactions table if it doesn't exist"""
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS fraud_transactions (
            id SERIAL PRIMARY KEY,
            time_stamp BIGINT NOT NULL,
            v1 DECIMAL(20,10), v2 DECIMAL(20,10), v3 DECIMAL(20,10), v4 DECIMAL(20,10), v5 DECIMAL(20,10),
            v6 DECIMAL(20,10), v7 DECIMAL(20,10), v8 DECIMAL(20,10), v9 DECIMAL(20,10), v10 DECIMAL(20,10),
            v11 DECIMAL(20,10), v12 DECIMAL(20,10), v13 DECIMAL(20,10), v14 DECIMAL(20,10), v15 DECIMAL(20,10),
            v16 DECIMAL(20,10), v17 DECIMAL(20,10), v18 DECIMAL(20,10), v19 DECIMAL(20,10), v20 DECIMAL(20,10),
            v21 DECIMAL(20,10), v22 DECIMAL(20,10), v23 DECIMAL(20,10), v24 DECIMAL(20,10), v25 DECIMAL(20,10),
            v26 DECIMAL(20,10), v27 DECIMAL(20,10), v28 DECIMAL(20,10),
            amount DECIMAL(10,2) NOT NULL,
            class INTEGER NOT NULL CHECK (class IN (0, 1)),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_time ON fraud_transactions(time_stamp);
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_class ON fraud_transactions(class);
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_amount ON fraud_transactions(amount);
        CREATE INDEX IF NOT EXISTS idx_fraud_transactions_created_at ON fraud_transactions(created_at);
        """
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_table_sql)
            logger.info("✅ Table fraud_transactions created/verified")
        except Exception as e:
            logger.error(f"❌ Failed to create table: {e}")
            raise
    
    def load_csv_data(self) -> pd.DataFrame:
        """Load and prepare CSV data"""
        try:
            # Read CSV file
            df = pd.read_csv(self.csv_file_path)
            
            # Rename columns to match our table structure
            df.columns = ['time_stamp'] + [f'v{i}' for i in range(1, 29)] + ['amount', 'class']
            
            # Shuffle data to simulate random order
            df = df.sample(frac=1).reset_index(drop=True)
            
            logger.info(f"✅ Loaded {len(df)} records from CSV")
            logger.info(f"📊 Fraud distribution: {df['class'].value_counts().to_dict()}")
            
            return df
        except Exception as e:
            logger.error(f"❌ Failed to load CSV data: {e}")
            raise
    
    def insert_batch(self, batch_data: pd.DataFrame):
        """Insert a batch of records into the database"""
        insert_sql = """
        INSERT INTO fraud_transactions (
            time_stamp, v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15,
            v16, v17, v18, v19, v20, v21, v22, v23, v24, v25, v26, v27, v28, amount, class
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
        )
        """
        
        try:
            cursor = self.connection.cursor()
            
            # Prepare batch data
            batch_values = []
            for _, row in batch_data.iterrows():
                values = tuple(row.values)
                batch_values.append(values)
            
            # Execute batch insert
            cursor.executemany(insert_sql, batch_values)
            
            fraud_count = batch_data[batch_data['class'] == 1].shape[0]
            normal_count = batch_data[batch_data['class'] == 0].shape[0]
            
            logger.info(f"📥 Inserted batch of {len(batch_data)} records "
                       f"(🚨 {fraud_count} fraud, ✅ {normal_count} normal)")
            
        except Exception as e:
            logger.error(f"❌ Failed to insert batch: {e}")
            raise
    
    def simulate_stream(self, total_records: Optional[int] = None):
        """Simulate streaming data insertion"""
        try:
            # Connect to database
            self.connect_to_db()
            
            # Create table if needed
            self.create_table_if_not_exists()
            
            # Load CSV data
            df = self.load_csv_data()
            
            # Limit records if specified
            if total_records:
                df = df.head(total_records)
                logger.info(f"🎯 Limited to {total_records} records for simulation")
            
            total_batches = len(df) // self.batch_size + (1 if len(df) % self.batch_size > 0 else 0)
            
            logger.info(f"🚀 Starting stream simulation:")
            logger.info(f"   📋 Total records: {len(df)}")
            logger.info(f"   📦 Batch size: {self.batch_size}")
            logger.info(f"   🔄 Total batches: {total_batches}")
            logger.info(f"   ⏱️  Delay range: {self.delay_range[0]}-{self.delay_range[1]} seconds")
            
            # Process in batches
            for i in range(0, len(df), self.batch_size):
                batch_end = min(i + self.batch_size, len(df))
                batch_data = df.iloc[i:batch_end]
                
                # Insert batch
                self.insert_batch(batch_data)
                
                # Progress tracking
                batch_num = (i // self.batch_size) + 1
                progress = (batch_num / total_batches) * 100
                logger.info(f"📊 Progress: {batch_num}/{total_batches} batches ({progress:.1f}%)")
                
                # Random delay between batches (except for last batch)
                if batch_end < len(df):
                    delay = random.uniform(self.delay_range[0], self.delay_range[1])
                    logger.info(f"⏳ Waiting {delay:.1f} seconds before next batch...")
                    time.sleep(delay)
            
            logger.info("🎉 Stream simulation completed successfully!")
            
        except KeyboardInterrupt:
            logger.info("⏹️  Stream simulation interrupted by user")
        except Exception as e:
            logger.error(f"❌ Stream simulation failed: {e}")
            raise
        finally:
            self.disconnect_from_db()
    
    def get_table_stats(self):
        """Get current table statistics"""
        try:
            self.connect_to_db()
            cursor = self.connection.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) FROM fraud_transactions")
            total_count = cursor.fetchone()[0]
            
            # Get fraud distribution
            cursor.execute("SELECT class, COUNT(*) FROM fraud_transactions GROUP BY class ORDER BY class")
            distribution = cursor.fetchall()
            
            # Get recent records
            cursor.execute("""
                SELECT COUNT(*) FROM fraud_transactions 
                WHERE created_at >= NOW() - INTERVAL '1 hour'
            """)
            recent_count = cursor.fetchone()[0]
            
            logger.info(f"📊 Table Statistics:")
            logger.info(f"   📋 Total records: {total_count}")
            logger.info(f"   🕐 Records in last hour: {recent_count}")
            logger.info(f"   📈 Distribution:")
            for class_val, count in distribution:
                class_name = "Normal" if class_val == 0 else "Fraud"
                percentage = (count / total_count * 100) if total_count > 0 else 0
                logger.info(f"      {class_name}: {count} ({percentage:.2f}%)")
            
        except Exception as e:
            logger.error(f"❌ Failed to get table stats: {e}")
        finally:
            self.disconnect_from_db()

def main():
    # Database configuration
    db_config = {
        'host': 'localhost',
        'port': 5434,
        'database': 'fraud_detection',
        'user': 'postgres',
        'password': 'postgres'
    }
    
    # CSV file path
    csv_file_path = '/home/always/FSDS-Lab/Fraud-Detection-Project/raw-data/archive/fake-stream-data.csv'
    
    # Check if CSV file exists
    if not os.path.exists(csv_file_path):
        logger.error(f"❌ CSV file not found: {csv_file_path}")
        sys.exit(1)
    
    # Create simulator
    simulator = FraudDataStreamSimulator(
        csv_file_path=csv_file_path,
        db_config=db_config,
        batch_size=2,# Insert 5 records at a time
        delay_range=(8, 15)  # Wait 2-8 seconds between batches
    )
    
    # Check command line arguments
    if len(sys.argv) > 1:
        if sys.argv[1] == 'stats':
            simulator.get_table_stats()
            return
        elif sys.argv[1] == 'simulate':
            records_limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
            simulator.simulate_stream(total_records=records_limit)
            return
    
    # Default: run simulation
    logger.info("🎮 Fraud Detection Stream Simulator")
    logger.info("Usage:")
    logger.info("  python stream_simulator.py simulate [limit]  # Run simulation")
    logger.info("  python stream_simulator.py stats            # Show table stats")
    logger.info("")
    
    # Ask user what to do
    choice = input("Choose action (1=simulate, 2=stats, 3=simulate with limit): ").strip()
    
    if choice == '1':
        simulator.simulate_stream()
    elif choice == '2':
        simulator.get_table_stats()
    elif choice == '3':
        limit = int(input("Enter record limit: "))
        simulator.simulate_stream(total_records=limit)
    else:
        logger.info("Invalid choice. Running default simulation...")
        simulator.simulate_stream(total_records=100)  # Default to 100 records

if __name__ == "__main__":
    main()

    # python scripts/stream_simulator.py