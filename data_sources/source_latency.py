# Python script to deploy source containers with latency
import time
import subprocess
import random
import sys

# Function to generate random latencies
def generate_random_latencies(num_containers):
    source_latencies = {}
    for i in range(num_containers):
        source_name = f'latency{i+1}'
        latency = round(random.uniform(0.0, 1.0), 1)
        source_latencies[source_name] = latency
    return source_latencies

# Function to simulate latencies
def simulate_latencies(image_name, container_prefix, num_containers):
    source_latencies = generate_random_latencies(num_containers)
    while True:
        # Apply latency for each source
        for source_name, latency in source_latencies.items():
            time.sleep(latency)
            # Run Docker containers
            run_containers(image_name, container_prefix, num_containers)

# Build Docker image
def build_image(image_name, dockerfile):
    command = ['docker', 'build', '-t', image_name, '-f', dockerfile, '.']
    subprocess.run(command)

# Run Docker containers
def run_containers(image_name, container_prefix, num_containers):
    for i in range(num_containers):
        container_name = f'{container_prefix}-{i+1}'
        command = ['docker', 'run', '-d', '--name', container_name, image_name]
        subprocess.run(command)

def broker_choice(choice):
    if choice.lower() == 'kafka':
        dockerfile = 'Dockerfile.kafka'
        container_prefix = 'kafka-source'
        image_name = 'kafka-metrics'
    elif choice.lower() == 'rabbitmq':
        dockerfile = 'Dockerfile.rabbitmq'
        container_prefix = 'rabbitmq-source'
        image_name = 'rabbitmq-metrics'
    else:
        print("Invalid choice. Exiting...")
        exit(1)
    return dockerfile, container_prefix, image_name

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 source_latency.py <kafka/rabbitmq> <num_containers>")
        sys.exit(1)

    # Number of containers to run and broker choice
    num_containers = int(sys.argv[1])
    choice = sys.argv[2]

    dockerfile, container_prefix, image_name = broker_choice(choice)

    # Build Docker image
    build_image(image_name, dockerfile)

    # Simulate latencies and run Docker containers
    simulate_latencies(image_name, container_prefix, num_containers)
