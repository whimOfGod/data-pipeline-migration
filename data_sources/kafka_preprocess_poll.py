from kafka import KafkaConsumer, KafkaProducer
import sys
import json
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import time

# Kafka configuration
bootstrap_servers = "163.173.170.86:9092" #"172.16.9.157:9092"
#bootstrap_servers = "163.173.170.189:9095"
cpu_consumer_topic = 'collected_cpu_topic' 
cpu_consumer_group = 'collected_cpu_group'
cpu_producer_topic = 'preprocessed_cpu_topic'
memory_consumer_topic = 'collected_memory_topic' 
memory_consumer_group = 'collected_memory_group'
memory_producer_topic = 'preprocessed_memory_topic'
batch_size = 500 # number of messages to consume in a batch

# Create Kafka consumer and producer
cpu_consumer = KafkaConsumer(cpu_consumer_topic, bootstrap_servers=bootstrap_servers, enable_auto_commit=True, auto_offset_reset='earliest', group_id=cpu_consumer_group)
memory_consumer = KafkaConsumer(memory_consumer_topic, bootstrap_servers=bootstrap_servers, enable_auto_commit=True, auto_offset_reset='earliest', group_id=memory_consumer_group)
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, api_version=(0, 10))

# Function to decode the message value
def decode_message(batch):
    messages = []
    for tp, records in batch.items():
        for record in records:
            # Decode the message value
            data = record.value.decode('utf-8')
            messages.append(data)
    return messages

# Function to create sequences; Receives a 3D array normalized values and returns a 3D array of sequences
def create_sequences(values, lookback=2):
    output = []
    for i in range(len(values) - lookback + 1):
        output.append(values[i: (i + lookback)])
    return np.stack(output)

# Function to concatenate feautures into a single array per timestamp_col (integer so we have more values per time window (1 second))
def extract_metrics(batch_list,resource_name):
    output_data = {}

    for data in batch_list:
        json_data = json.loads(data)
        source_id = json_data['source_id']
        timestamp_col = json_data['timestamp_col']
        timestamp_train = int(json_data['timestamp_col'])
        timestamp_pub = json_data['timestamp_pub']
        metrics = {key: value for key, value in json_data.items() if key.startswith(resource_name)}
        features = len(metrics.values())

        if timestamp_train not in output_data:
            output_data[timestamp_train] = {
                'metrics': [],
                'source_id': source_id,
                'timestamp_col': timestamp_col,
                'timestamp_pub': timestamp_pub
            }

        output_data[timestamp_train]['metrics'].extend(list(metrics.values()))

    return output_data, features

# Function to incrementally min-max normalize the data
def incremental_min_max_normalize(feature_batch_dict, global_max, global_min):
    normalized_features_dict = {}

    for timestamp_train, feature_batch_array in feature_batch_dict.items():
        metrics_array = feature_batch_array['metrics']    
        source_id = feature_batch_array['source_id']
        timestamp_col = feature_batch_array['timestamp_col']
        timestamp_pub = feature_batch_array['timestamp_pub']
        
        local_max = np.max(metrics_array)
        local_min = np.min(metrics_array)

        if local_max > global_max:
            global_max = local_max

        if local_min < global_min:
            global_min = local_min

        # Check if the range is zero
        if global_max == global_min:
            range_value = 1  # Assign a default range value of 1
        else:
            range_value = global_max - global_min
        norm_batch_array = (metrics_array - global_min) / range_value
        
        if timestamp_train not in normalized_features_dict:
            normalized_features_dict[timestamp_train] = {
                'metrics': [],
                'source_id': source_id,
                'timestamp_col': timestamp_col,
                'timestamp_pub': timestamp_pub
            }

        normalized_features_dict[timestamp_train]['metrics'].extend(norm_batch_array.tolist())

    return normalized_features_dict, global_max, global_min

def preprocess(batch_list,resource_name,timestamp_prep_start):
    sequence_dict = {}
    global_max = -np.inf
    global_min = np.inf
    batch_dict, features = extract_metrics(batch_list,resource_name)
    lookback = 2
    
    norm_batch_dict, global_max, global_min = incremental_min_max_normalize(batch_dict, global_max, global_min)
    
    for timestamp_train, norm_batch_array in norm_batch_dict.items():
        metrics_norm_batch_array = norm_batch_array['metrics'] 
        source_id = norm_batch_array['source_id']
        timestamp_col = norm_batch_array['timestamp_col']
        timestamp_pub = norm_batch_array['timestamp_pub']
        
        norm_batch_2d = np.array(metrics_norm_batch_array).reshape(-1, features)
        print('norm_batch_2d.shape',norm_batch_2d.shape)
        
        if norm_batch_2d.shape[0] > 1:
            sequence = create_sequences(norm_batch_2d)
            
            if timestamp_train not in sequence_dict:
                sequence_dict[timestamp_train] = {
                    'metrics': [],
                    'source_id': source_id,
                    'timestamp_col': timestamp_col,
                    'timestamp_pub': timestamp_pub,
                    'timestamp_prep_start': timestamp_prep_start,
                    'timestamp_prep_end': time.time()
                }

            sequence_dict[timestamp_train]['metrics'].extend(sequence)

    return sequence_dict

def convert_ndarray_to_list(data):
    if isinstance(data, np.ndarray):
        return data.tolist()
    elif isinstance(data, list):
        return [convert_ndarray_to_list(item) for item in data]
    elif isinstance(data, dict):
        return {key: convert_ndarray_to_list(value) for key, value in data.items()}
    else:
        return data

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 kafka_preprocess.py <resource_name>")
        sys.exit(1)
    resource_name = sys.argv[1]
    if resource_name == 'cpu':
        consumer = cpu_consumer
        producer_topic = cpu_producer_topic
    elif resource_name == 'memory':
        consumer = memory_consumer
        producer_topic = memory_producer_topic

    # Consume messages in batches
    print("Consuming messages...")
    while True:
        #timestamp_prep_start = time.time()
        batch = consumer.poll(timeout_ms=1000, max_records=batch_size)
        timestamp_prep_start = time.time()
        if not batch:
            print('no batch')
            continue
        else:
            batch_decoded = decode_message(batch)
            print('batch len:',len(batch_decoded))
            if len(batch_decoded) == 1:
                #print('not enough messages: 1')
                continue
            if len(batch_decoded) > 1:
                sequences_dict = preprocess(batch_decoded,resource_name,timestamp_prep_start)
                # Publish to Kafka topic
                sequences_dict_converted = convert_ndarray_to_list(sequences_dict)
                json_value = json.dumps(sequences_dict_converted)
                print('json_value',json_value)
                print()
                producer.send(producer_topic, value=json_value.encode('utf-8'))
