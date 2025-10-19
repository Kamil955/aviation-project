# flink_job.py

import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

def parse_flight_data(json_string):
    """
    Parses a JSON string into a Python dictionary.
    Returns None if the string is not a valid JSON.
    """
    try:
        return json.loads(json_string)
    except json.JSONDecodeError:
        return None

def format_flight_output(flight):
    """
    Safely formats flight data into a readable string.
    Handles cases where 'callsign' might be None.
    """
    callsign = flight.get('callsign')
    # Check if callsign is not None before stripping
    display_callsign = callsign.strip() if callsign else 'N/A'
    altitude = flight.get('baro_altitude', 'Unknown')
    return f"Processing airborne flight: {display_callsign} at altitude {altitude} m"

def main():
    # 1. Set up the Flink execution environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # 2. Configure the Kafka source
    kafka_consumer = FlinkKafkaConsumer(
        topics='flights_raw',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'kafka:29092',
            'group.id': 'flink_consumer_group'
        }
    )
    kafka_consumer.set_start_from_earliest() # Start from the beginning of the topic

    # 3. Add the Kafka consumer as a source
    data_stream = env.add_source(kafka_consumer)

    # 4. Define the processing logic
    # STEP 4.1: Parse each JSON string into a Python dictionary
    parsed_stream = data_stream.map(parse_flight_data)

    # STEP 4.2: Filter out any records that failed to parse
    valid_stream = parsed_stream.filter(lambda record: record is not None)

    # STEP 4.3: Filter out aircraft that are on the ground
    airborne_stream = valid_stream.filter(lambda flight: not flight.get('on_ground', True))

    # STEP 4.4: Map the filtered data to a readable string using our safe function
    output_stream = airborne_stream.map(format_flight_output)
    
    # STEP 4.5: Print the final stream to the logs
    output_stream.print()

    # 5. Execute the Flink job
    print("Submitting Flink job to the cluster...")
    env.execute("Flight Data Processing Job")

if __name__ == '__main__':
    main()