#Python script to assign sources to brokers, based on EMMA algorithm
import random
import time

# Constants
BROKER_1 = "172.16.9.148:9092"
BROKER_2 = "172.16.9.149:9092"
THRESHOLD = 0.5  # Example threshold for latency change

# Initial source-broker assignment
broker1_sources = []
broker2_sources = []
g1_sources = []
g2_sources = []

def assign_source_to_broker(source_id, broker):
    if broker == BROKER_1:
        broker1_sources.append(source_id)
    elif broker == BROKER_2:
        broker2_sources.append(source_id)

def update_source_assignment(source_id, new_broker):
    global broker1_sources, broker2_sources
    if source_id in broker1_sources:
        broker1_sources.remove(source_id)
    elif source_id in broker2_sources:
        broker2_sources.remove(source_id)
    assign_source_to_broker(source_id, new_broker)

def calculate_assignment_counts(total_sources):
    count_g1 = int(total_sources / 2)  # Adjust as per distribution; 2 is our current distribution
    count_g2 = total_sources - count_g1
    return count_g1, count_g2

def monitor_latency():
    while True:
        # Simulating latency changes
        time.sleep(2)
        source_id = random.randint(1, 100)
        new_latency = random.uniform(0.0, 1.0)

        # Determine the current broker and latency group of the source
        current_broker = BROKER_1 if source_id in broker1_sources else BROKER_2
        current_group = g1_sources if source_id in g1_sources else g2_sources

        # Determine the new latency group based on the updated latency
        new_group = g1_sources if new_latency <= THRESHOLD else g2_sources

        if current_group != new_group:
            print(f"Source {source_id} switched from {current_group} to {new_group}")

            # Update the source-broker assignment
            update_source_assignment(source_id, current_broker if current_broker != BROKER_1 else BROKER_2)

            # Update the latency group lists
            current_group.remove(source_id)
            new_group.append(source_id)

            print(f"Broker 1 lenght: {len(broker1_sources)}, sources: {broker1_sources}")
            print(f"Broker 2 lenght: {len(broker2_sources)}, sources: {broker2_sources}")
            print(f"Group 1 lenght: {len(g1_sources)}, sources: {g1_sources}")
            print(f"Group 2 lenght: {len(g2_sources)}, sources: {g2_sources}")

if __name__ == "__main__":
    # Generate a list of source ids and their corresponding latencies (for simulation)
    source_ids = list(range(0, 100))
    source_latencies = {source_id: random.uniform(0.0, 1.0) for source_id in source_ids}
    print(source_ids, source_latencies)
    print('Source ids and latencies #:',len(source_ids), len(source_latencies),'\n')

    # Sort the source ids based on latencies in ascending order
    sorted_sources = sorted(source_ids, key=lambda source_id: source_latencies[source_id])

    # Assign source ids to latency groups
    count_broker1 = 0
    count_broker2 = 0
    for source_id in sorted_sources:
        latency = source_latencies[source_id]
        if latency <= THRESHOLD:
            g1_sources.append(source_id)
            assign_source_to_broker(source_id, BROKER_1)
            count_broker1 += 1
        else:
            count_broker2 += 1
            g2_sources.append(source_id)
            assign_source_to_broker(source_id, BROKER_2)
    print('count_b1:',count_broker1)
    print('count_b2:',count_broker2)

    """# Calculate the number of source ids to be assigned to each broker
    # cuts off the last few sources to keep to a maximum of half length of the source list assigned per broker
    count_g1, count_g2 = calculate_assignment_counts(len(source_ids))
    count_broker1 = min(count_g1, len(g1_sources))
    count_broker2 = min(count_g2, len(g2_sources))
    print('# source ids to be assigned to each broker:')
    print(count_broker1, count_broker2)"""

    """# Divide them equally between the brokers
    half_size = len(source_ids) // 2
    broker1_sources = source_ids[:half_size]
    broker2_sources = source_ids[half_size:]
    print(len(broker1_sources), len(broker2_sources))"""

    # Update the broker-source assignments based on calculated counts
    # it's not a perfect 50-50 assignment, but it's close enough and keeps latency groups accurate
    broker1_sources = g1_sources[:count_broker1]
    broker2_sources = g2_sources[:count_broker2]
    print('Update the broker-source assignments based on calculated counts')
    print(len(broker1_sources), len(broker2_sources))
    #call to metrics collector script

    # Start monitoring the latencies and making dynamic adjustments
    monitor_latency()
