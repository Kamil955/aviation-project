# flink_job.py

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

def main():
    # 1. Set up the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. Configure the data source - our Kafka topic
    kafka_consumer = FlinkKafkaConsumer(
        topics='flights_raw',
        deserialization_schema=SimpleStringSchema(),
        properties={
            # --- ZMIEŃ PORT W TEJ LINIJCE ---
            'bootstrap.servers': 'kafka:29092', 
            'group.id': 'flink_consumer_group'
        }
    )

    # 3. Add the Kafka consumer as a source
    data_stream = env.add_source(kafka_consumer)

    # 4. Define the operation: print data to logs
    data_stream.print()

    # 5. Execute the Flink job
    print("Submitting Flink job to the cluster...")
    env.execute("Kafka to Log Example Job")

if __name__ == '__main__':
    main()