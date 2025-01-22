# Python script to setup a Kafka cluster

import subprocess

kafka_brokers = ["172.16.9.148:9092", "172.16.9.149:9092"]

def setup_kafka_cluster():
    # Configure Kafka broker properties
    for broker in kafka_brokers:
        subprocess.run([
            'sed', '-i', 's/^#listeners=PLAINTEXT:\/\/:9092/listeners=PLAINTEXT:\/\/{0}:9092/g'.format(broker),
            '/path/to/kafka/config/server.properties'
        ])

        subprocess.run([
            'sed', '-i', 's/^#advertised.listeners=PLAINTEXT:\/\/your.host.name:9092/advertised.listeners=PLAINTEXT:\/\/{0}:9092/g'.format(broker),
            '/path/to/kafka/config/server.properties'
        ])

    # Start Kafka brokers
    for broker in kafka_brokers:
        subprocess.run([
            '/path/to/kafka/bin/kafka-server-start.sh', '-daemon',
            '/path/to/kafka/config/server.properties'
        ])

    print("Kafka cluster setup completed successfully.")


if __name__ == "__main__":
    setup_kafka_cluster()