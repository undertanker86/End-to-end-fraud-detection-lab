import json
import logging
from pyflink.common import Types
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_simple_kafka_test():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    
    jars = [
        'file:///opt/flink/lib/flink-connector-kafka-1.17.1.jar',
        'file:///opt/flink/lib/kafka-clients-3.4.0.jar'
    ]
    
    for jar in jars:
        env.add_jars(jar)
    
    logger.info('🚀 Starting Simple Kafka Test')
    
    kafka_props = {
        'bootstrap.servers': 'broker:29092',
        'group.id': 'simple-kafka-test',
        'auto.offset.reset': 'latest'
    }
    
    kafka_consumer = FlinkKafkaConsumer(
        topics=['fraud_detection_server.public.fraud_transactions'],
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    kafka_consumer.set_start_from_latest()
    
    kafka_stream = env.add_source(kafka_consumer)
    kafka_stream.map(
        lambda x: f'📨 Message length: {len(x)}',
        output_type=Types.STRING()
    ).print()
    
    logger.info('📊 Pipeline: Kafka -> Print')
    env.execute('Simple Kafka Test')

if __name__ == '__main__':
    create_simple_kafka_test()
