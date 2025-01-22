# Python script to start the Kafka server and ZooKeeper server

import os
import subprocess
import signal
import sys





if __name__ == '__main__':
    
    if len(sys.argv) < 2:
        print("Usage: python3 NDBF.py <kafka_dir>")
        sys.exit(1)
    kafka_dir = str(sys.argv[1])
    os.chdir(kafka_dir)
    
    # Start ZooKeeper
    zookeeper_process = subprocess.Popen(["bin/zookeeper-server-start.sh", "config/zookeeper.properties"])
    
    # Start Kafka Server
    kafka_process = subprocess.Popen(["bin/kafka-server-start.sh", "config/server.properties"])
    
    # Wait for user input to stop Kafka
    input("Kafka server is running. Press Enter to stop.")
    
    # Stop Kafka Server
    kafka_process.send_signal(signal.SIGTERM)
    kafka_process.wait()
    
    # Stop ZooKeeper Server
    zookeeper_process.send_signal(signal.SIGTERM)
    zookeeper_process.wait()
    
    print("Kafka server stopped.")