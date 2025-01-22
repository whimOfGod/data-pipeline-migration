# Python script to check Kafka topics and messages per topc/ group

import subprocess
import sys

# topic_name = 'test_topic' # update to actual topic name
# group_id = 'test_group' # update to actual group name
bootstrap_servers = ["172.16.9.148:9092", "172.16.9.149:9092"]

def check_kafka_topic(group_id, bootstrap_servers):
    # Show messages per topic and group
    for bootstrap_server in bootstrap_servers:
        cmd = f"kafka_2.13-3.4.0/bin/kafka-consumer-groups.sh --bootstrap-server {bootstrap_server} --describe --group {group_id}"
        subprocess.run(cmd, shell=True)

def list_kafka_topics(bootstrap_servers):
    # List topics + info
    for bootstrap_server in bootstrap_servers:
        cmd = f"kafka_2.13-3.4.0/bin/kafka-topics.sh --bootstrap-server {bootstrap_server} --describe"
        subprocess.run(cmd, shell=True)

def delete_kafka_topic(topic_name, bootstrap_servers):
    # Delete topic
    for bootstrap_server in bootstrap_servers:
        cmd = f"kafka_2.13-3.4.0/bin/kafka-topics.sh --bootstrap-server {bootstrap_server} --delete --topic {topic_name}"
        subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python3 kafka_checks.py <action> <topic_name> <group_id>")
        sys.exit(1)

    action = sys.argv[1]
    topic_name = sys.argv[2]
    group_id = sys.argv[3]
   
    if action == 'check':
        list_kafka_topics(bootstrap_servers)
        check_kafka_topic(group_id, bootstrap_servers)
    elif action == 'delete':
        delete_kafka_topic(topic_name, bootstrap_servers)
        print('Topic deleted. You will need to create the topic again and re-run consumer script to create the group id')
    else:
        print("Invalid action!")
