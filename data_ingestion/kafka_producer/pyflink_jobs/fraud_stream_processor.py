import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

import pandas as pd
import numpy as np
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig
from pyflink.common.serialization import SimpleStringSchema, Encoder
from pyflink.datastream.functions import MapFunction, ProcessFunction
from pyflink.datastream.window import TumblingProcessingTimeWindows

# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FraudTransaction:
    """Data class for fraud transactions from CDC"""
    
    def __init__(self, **kwargs):
        self.id = kwargs.get('id')
        self.time_stamp = kwargs.get('time_stamp')
        
        # PCA features V1-V28
        for i in range(1, 29):
            setattr(self, f'v{i}', kwargs.get(f'v{i}', 0.0))
        
        self.amount = kwargs.get('amount', 0.0)
        self.fraud_class = kwargs.get('class', 0)
        self.created_at = kwargs.get('created_at')
        self.processed_at = kwargs.get('processed_at')
    
    @classmethod
    def from_debezium_json(cls, json_str: str) -> Optional['FraudTransaction']:
        try:
            data = json.loads(json_str)
            # Try Debezium format first
            payload = data.get('payload', {})
            after = payload.get('after', {})
            if after:
                return cls(**after)
            # Fallback: flat JSON
            if 'id' in data and 'amount' in data:
                return cls(**data)
            return None
        except Exception as e:
            logger.error(f"Error parsing transaction: {e}")
            return None


class MLFeatureRecord:
    """Enhanced feature record consistent with Airflow DAGs logic"""
    
    def __init__(self):
        # Original PCA features (V1-V28)
        for i in range(1, 29):
            setattr(self, f'v{i}', 0.0)
        
        # Amount features (from bronze_to_silver_etl.py)
        self.amount = 0.0
        self.amount_log = 0.0
        self.amount_percentile = 0.0
        self.amount_zscore = 0.0
        
        # Time features (from silver_to_gold_etl.py)
        self.hour = 0
        self.day_of_week = 0
        self.is_weekend = 0
        self.hour_sin = 0.0
        self.hour_cos = 0.0
        self.day_of_week_sin = 0.0
        self.day_of_week_cos = 0.0
        
        # Rolling statistics
        self.amount_rolling_mean = 0.0
        self.amount_rolling_std = 0.0
        self.amount_rolling_min = 0.0
        self.amount_rolling_max = 0.0
        
        # Interaction features (from silver_to_gold_etl.py)
        self.v1_amount_interaction = 0.0
        self.v2_amount_interaction = 0.0
        self.v3_amount_interaction = 0.0
        self.v4_amount_interaction = 0.0
        self.v5_amount_interaction = 0.0
        
        # Statistical features
        self.outlier_ratio = 0.0
        self.data_quality_score = 1.0
        self.transaction_velocity = 0.0
        
        # PCA statistics
        self.pca_mean = 0.0
        self.pca_std = 0.0
        self.pca_skewness = 0.0
        self.pca_kurtosis = 0.0
        
        # Advanced features (consistent with silver_to_gold_etl.py)
        self.amount_range_flag = 0
        self.time_cluster = 0
        self.risk_score = 0.0
        
        # Target and metadata
        self.fraud_class = 0
        self.feature_version = "1.0"
        self.created_at = datetime.now().isoformat()
        self.data_source = "stream_layer"
        self.transaction_id = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for output"""
        return {k: v for k, v in self.__dict__.items() if not k.startswith('_')}


class JsonToTransactionMapper(MapFunction):
    """Map Debezium CDC JSON to FraudTransaction object"""
    
    def map(self, value: str) -> Optional[FraudTransaction]:
        return FraudTransaction.from_debezium_json(value)


class FeatureEngineeringMapper(MapFunction):
    """Apply feature engineering - consistent with bronze_to_silver_etl.py"""
    
    def map(self, transaction: FraudTransaction) -> MLFeatureRecord:
        if transaction is None:
            return None
        
        record = MLFeatureRecord()
        record.transaction_id = transaction.id
        
        # Copy original PCA features (V1-V28)
        for i in range(1, 29):
            v_value = getattr(transaction, f'v{i}', 0.0)
            setattr(record, f'v{i}', float(v_value) if v_value is not None else 0.0)
        
        # Amount transformations (like bronze_to_silver_etl.py)
        record.amount = float(transaction.amount) if transaction.amount else 0.0
        record.amount_log = np.log1p(record.amount)
        record.amount_percentile = self._calculate_amount_percentile(record.amount)
        record.amount_zscore = self._calculate_amount_zscore(record.amount)
        
        # Amount range classification
        record.amount_range_flag = self._classify_amount_range(record.amount)
        
        # Time-based features
        record = self._engineer_time_features(transaction, record)
        
        # Interaction features (like silver_to_gold_etl.py)
        record.v1_amount_interaction = record.v1 * record.amount_log
        record.v2_amount_interaction = record.v2 * record.amount_log
        record.v3_amount_interaction = record.v3 * record.amount_log
        record.v4_amount_interaction = record.v4 * record.amount_log
        record.v5_amount_interaction = record.v5 * record.amount_log
        
        # PCA statistics
        pca_features = [getattr(record, f'v{i}') for i in range(1, 29)]
        record = self._calculate_pca_statistics(record, pca_features)
        
        # Data quality and outlier detection
        record.data_quality_score = self._calculate_data_quality_score(transaction)
        record.outlier_ratio = self._calculate_outlier_ratio(pca_features)
        
        # Risk scoring
        record.risk_score = self._calculate_risk_score(record)
        
        # Target variable
        record.fraud_class = transaction.fraud_class
        
        return record
    
    def _calculate_amount_percentile(self, amount: float) -> float:
        """Calculate amount percentile based on training data distribution"""
        # Based on typical credit card transaction distribution
        percentiles = [0, 5, 25, 50, 100, 200, 500, 1000]
        ranks = [0.1, 0.25, 0.5, 0.7, 0.85, 0.95, 0.99, 1.0]
        
        for i, threshold in enumerate(percentiles):
            if amount <= threshold:
                return ranks[i]
        return 1.0
    
    def _calculate_amount_zscore(self, amount: float) -> float:
        """Calculate Z-score for amount"""
        # Statistics from training data
        mean_amount = 88.35
        std_amount = 250.12
        return (amount - mean_amount) / std_amount if std_amount > 0 else 0.0
    
    def _classify_amount_range(self, amount: float) -> int:
        """Classify amount into ranges"""
        if amount < 10:
            return 0  # Very small
        elif amount < 100:
            return 1  # Small
        elif amount < 500:
            return 2  # Medium
        elif amount < 1000:
            return 3  # Large
        else:
            return 4  # Very large
    
    def _engineer_time_features(self, transaction: FraudTransaction, record: MLFeatureRecord) -> MLFeatureRecord:
        """Engineer time-based features like silver_to_gold_etl.py"""
        try:
            if transaction.created_at:
                dt = None
                if isinstance(transaction.created_at, str):
                    dt_str = transaction.created_at.replace(' ', 'T')
                    if 'T' not in dt_str:
                        dt_str += 'T00:00:00'
                    dt = datetime.fromisoformat(dt_str.split('+')[0])
                elif isinstance(transaction.created_at, (int, float)):
                    # Assume microseconds or seconds, try both
                    ts = int(transaction.created_at)
                    if ts > 1e12:  # microseconds
                        dt = datetime.fromtimestamp(ts / 1e6)
                    elif ts > 1e9:  # milliseconds
                        dt = datetime.fromtimestamp(ts / 1e3)
                    else:  # seconds
                        dt = datetime.fromtimestamp(ts)
                if dt:
                    record.hour = dt.hour
                    record.day_of_week = dt.weekday()
                    record.is_weekend = 1 if dt.weekday() >= 5 else 0
                    record.hour_sin = np.sin(2 * np.pi * record.hour / 24)
                    record.hour_cos = np.cos(2 * np.pi * record.hour / 24)
                    record.day_of_week_sin = np.sin(2 * np.pi * record.day_of_week / 7)
                    record.day_of_week_cos = np.cos(2 * np.pi * record.day_of_week / 7)
                    if 9 <= record.hour <= 17:
                        record.time_cluster = 1  # Business hours
                    elif 18 <= record.hour <= 22:
                        record.time_cluster = 2  # Evening
                    else:
                        record.time_cluster = 0  # Night/Early morning
        except Exception as e:
            logger.warning(f"Error processing time features: {e}")
            record.hour = 12
            record.day_of_week = 1
            record.is_weekend = 0
            record.hour_sin = 0.0
            record.hour_cos = 1.0
            record.day_of_week_sin = 0.0
            record.day_of_week_cos = 1.0
            record.time_cluster = 1
        return record
    
    def _calculate_pca_statistics(self, record: MLFeatureRecord, pca_features: list) -> MLFeatureRecord:
        """Calculate PCA feature statistics"""
        try:
            pca_array = np.array(pca_features)
            record.pca_mean = float(np.mean(pca_array))
            record.pca_std = float(np.std(pca_array))
            
            # Skewness and kurtosis
            if len(pca_features) > 0:
                series = pd.Series(pca_features)
                record.pca_skewness = float(series.skew())
                record.pca_kurtosis = float(series.kurtosis())
        except Exception as e:
            logger.warning(f"Error calculating PCA statistics: {e}")
            record.pca_mean = 0.0
            record.pca_std = 1.0
            record.pca_skewness = 0.0
            record.pca_kurtosis = 0.0
        
        return record
    
    def _calculate_data_quality_score(self, transaction: FraudTransaction) -> float:
        """Calculate data quality score"""
        score = 1.0
        
        # Check amount validity
        if transaction.amount is None or transaction.amount <= 0:
            score -= 0.3
        
        # Check for extreme amounts
        if transaction.amount and transaction.amount > 25000:
            score -= 0.2
        
        # Check PCA features for missing/extreme values
        missing_count = 0
        extreme_count = 0
        for i in range(1, 29):
            v_val = getattr(transaction, f'v{i}', None)
            if v_val is None:
                missing_count += 1
            elif abs(v_val) > 10:  # Extreme PCA values
                extreme_count += 1
        
        score -= (missing_count / 28) * 0.3
        score -= (extreme_count / 28) * 0.2
        
        return max(0.0, score)
    
    def _calculate_outlier_ratio(self, pca_features: list) -> float:
        """Calculate ratio of outlier PCA features"""
        outlier_count = sum(1 for v in pca_features if abs(v) > 3.0)
        return outlier_count / len(pca_features) if pca_features else 0.0
    
    def _calculate_risk_score(self, record: MLFeatureRecord) -> float:
        """Calculate risk score based on multiple factors"""
        risk_score = 0.0
        
        # Amount-based risk
        if record.amount > 1000:
            risk_score += 0.3
        elif record.amount > 500:
            risk_score += 0.2
        elif record.amount < 1:
            risk_score += 0.4  # Very small amounts can be suspicious
        
        # Time-based risk
        if record.time_cluster == 0:  # Night transactions
            risk_score += 0.2
        if record.is_weekend:
            risk_score += 0.1
        
        # Statistical risk
        if record.outlier_ratio > 0.5:
            risk_score += 0.3
        
        if record.data_quality_score < 0.7:
            risk_score += 0.2
        
        return min(1.0, risk_score)


class WindowProcessor(ProcessFunction):
    """Process windowed data for rolling statistics"""

    def __init__(self):
        self.window_data = []
        self.window_size = 100

    def process_element(self, value, ctx):
        """Process each element with rolling statistics"""
        if value is None:
            return

        # Add to window
        self.window_data.append(value)

        # Maintain window size
        if len(self.window_data) > self.window_size:
            self.window_data.pop(0)

        # Calculate rolling statistics
        amounts = [record.amount_log for record in self.window_data if record.amount_log is not None]

        if amounts:
            value.amount_rolling_mean = float(np.mean(amounts))
            value.amount_rolling_std = float(np.std(amounts))
            value.amount_rolling_min = float(np.min(amounts))
            value.amount_rolling_max = float(np.max(amounts))

        # Calculate transaction velocity (transactions per 5-minute window)
        value.transaction_velocity = len(self.window_data) / 5.0

        yield value
class JsonToTransactionMapper(MapFunction):
    def map(self, value: str) -> Optional[FraudTransaction]:
        logger.info(f"Received raw message: {value}")
        result = FraudTransaction.from_debezium_json(value)
        if result is None:
            logger.warning(f"Failed to parse message: {value}")
        return result

def create_fraud_stream_processor():
    from pyflink.common import Configuration

    """Create and configure the PyFlink streaming job"""

    # Tạo Configuration để set S3 endpoint
    config = Configuration()
    config.set_string("fs.s3a.endpoint", "http://minio:9000")
    config.set_string("fs.s3a.access.key", "minioadmin")
    config.set_string("fs.s3a.secret.key", "minioadmin")
    config.set_string("fs.s3a.path.style.access", "true")
    config.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")


    # Khởi tạo env với config
    env = StreamExecutionEnvironment.get_execution_environment(configuration=config)

    print("The configuration of the environment:")
    print(env)

    env.set_parallelism(1)
    env.enable_checkpointing(60000)

    logger.info("🚀 Starting PyFlink Fraud Detection Stream Processor")

    # Configure Kafka consumer
    kafka_props = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'pyflink-fraud-processor-test',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': 'true'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topics=['fraud_db.public.fraud_transactions'],
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )

    # Create data stream from Kafka
    kafka_stream = env.add_source(kafka_consumer)

    # Transform pipeline
    transaction_stream = kafka_stream.map(
        JsonToTransactionMapper(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    ).filter(lambda x: x is not None)

    feature_stream = transaction_stream.map(
        FeatureEngineeringMapper(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    processed_stream = feature_stream.process(
        WindowProcessor(),
        output_type=Types.PICKLED_BYTE_ARRAY()
    )

    # Convert to JSON strings
    json_stream = processed_stream.map(
        lambda record: json.dumps(record.to_dict(), default=str) if record else None,
        output_type=Types.STRING()
    ).filter(lambda x: x is not None)

    # Sink to MinIO Gold layer
    output_path = "s3a://gold/ml_features_train_stream/"

    file_sink = FileSink.for_row_format(
        base_path=output_path,
        encoder=Encoder.simple_string_encoder("UTF-8")
    ).with_output_file_config(
        OutputFileConfig.builder()
        .with_part_prefix("ml_features_train_stream")
        .with_part_suffix(".json")
        .build()
    ).build()

    json_stream.sink_to(file_sink)

    logger.info("📊 Pipeline configured: CDC -> Feature Engineering -> MinIO Gold")

    # Execute the job
    env.execute("PyFlink Fraud Detection Stream Processor")

if __name__ == "__main__":
    create_fraud_stream_processor()