# Python script to deploy Kafka

import subprocess

# Update System Packages
subprocess.run(["sudo", "apt", "update"])
subprocess.run(["sudo", "apt", "upgrade"])

# Download and Extract Apache Kafka
kafka_version = "2.13-3.9.0"
latest_kafka_version = "3.9.0"
kafka_url = f"https://dlcdn.apache.org/kafka/{latest_kafka_version}/kafka_{kafka_version}.tgz"
#kafka_url = f"https://archive.apache.org/kafka/{latest_kafka_version}/kafka_{kafka_version}.tgz"
#kafka_url = f"https://downloads.apache.org/kafka/{latest_kafka_version}/kafka_{kafka_version}.tgz"

subprocess.run(["wget", kafka_url])
subprocess.run(["tar", "-xzf", f"kafka_{kafka_version}.tgz"])
