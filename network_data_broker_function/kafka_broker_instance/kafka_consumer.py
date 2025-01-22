from kafka import KafkaConsumer
import time
import json

# Topic details
topic_name = 'test_topic'  # Replace with actual topic name

# Kafka parameters
bootstrap_servers = ["172.16.9.148:9092", "172.16.9.149:9092"]

def connect_kafka_consumer(bootstrap_servers, topic_name):
    # Create a Kafka consumer
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        enable_auto_commit=False,
        auto_offset_reset='earliest', # Consume from the beginning
        group_id='test_group' # Replace with actual consumer group ID
    )
    return consumer

def consume_messages(consumer):
    message_dict = {}  # Dictionary to store consumed messages
    print(f"Consuming Kafka messages from topic: {consumer.subscription()}. Press CTRL+C to exit.")
    for message in consumer:
        # Get the current timestamp as Unix timestamp
        current_time = time.time()

        # Process the received message
        message_value = message.value.decode()
        output_message = f"Kafka message consumed at: {current_time}, Message: {message_value}"
        print(output_message)

        # Create the message dictionary
        message_dict = {
            'timestamp_con': current_time,
            'sample': message_value
        }

        # Append the message to the consumed_messages list
        consumed_messages.append(message_dict)

        # Manually commit the offset to mark the message as consumed
        consumer.commit()

if __name__ == "__main__":
    # Connect to Kafka
    consumer = connect_kafka_consumer(bootstrap_servers, topic_name)

    consumed_messages = []  # List to store consumed messages

    while True:
        try:
            # Consumer
            consume_messages(consumer)
            #time.sleep(interval_seconds)
        except KeyboardInterrupt:
            # Handle keyboard interrupt (CTRL+C)
            print("Keyboard Interrupt, exiting...")
            break

    # Close the consumer connection
    consumer.close()

    # Save the consumed messages to a text file
    with open('kafka_messages_consumed.txt', 'w') as file:
        for message_dict in consumed_messages:
            file.write(json.dumps(message_dict) + '\n')
