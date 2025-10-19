# Dockerfile

# Use the official Flink image as our starting point
FROM apache/flink:1.18.1-scala_2.12-java17

# Switch to the root user to be able to install software
USER root

# Run system commands inside the image to install Python, Pip, and wget
RUN apt-get update && apt-get install -y python3 python3-pip wget

# Create a symbolic link so that the 'python' command points to 'python3'
RUN ln -s /usr/bin/python3 /usr/bin/python

# Install required Python libraries for PyFlink
RUN pip install protobuf
# --- NEW, CRUCIAL LINE ---
RUN pip install apache-flink

# Download the Flink Kafka Connector and its dependency (kafka-clients)
# and place them in the 'lib' directory, where Flink automatically loads all JARs.
RUN wget https://repo.maven.apache.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar -P /opt/flink/lib/
RUN wget https://repo.maven.apache.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar -P /opt/flink/lib/

# Good practice: switch back to the default, non-root user
USER flink