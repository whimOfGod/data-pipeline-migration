# Python script to create a topic in Kafka

from kafka.admin import KafkaAdminClient, NewTopic
import sys

# Bootstrap_server connection string
bootstrap_servers = ["172.16.9.148:9092", "172.16.9.149:9092"]

# Topic details
# topic_name = 'test_topic'
topic_partitions = 2 # update with actual number of partitions
topic_replication_factor = 2 # update with actual replication factor

def check_topic_exists(admin_client, topic_name):
    topic_metadata = admin_client.list_topics(topic_name)
    return topic_name in topic_metadata.topics

def create_kafka_topic(admin_client, topic_name, partitions, replication_factor):
    if check_topic_exists(admin_client, topic_name):
        print("Kafka topic already exists.")
    else:
        new_topic = NewTopic(name=topic_name, num_partitions=partitions, replication_factor=replication_factor)
        admin_client.create_topics(new_topics=[new_topic])
        print("Kafka topic creation completed successfully.")

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 kafka_topic.py <topic_name>")
        sys.exit(1)

    topic_name = sys.argv[1]

     # Create a Kafka admin client
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    # Create the Kafka topic
    create_kafka_topic(admin_client, topic_name, topic_partitions, topic_replication_factor)