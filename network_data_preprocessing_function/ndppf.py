from kafka import KafkaConsumer, KafkaProducer
import sys
import json
import numpy as np
from sklearn.preprocessing import MinMaxScaler
import time
import csv
import ntplib
import queue
from threading import Thread

# Kafka configuration
#iNDBF = "163.173.170.89:9092"
#eNDBF = "163.173.170.189:9092"
#cpu_consumer_topic = 'collected_cpu_topic' 
cpu_consumer_group = 'collected_cpu_group'
#cpu_producer_topic = 'preprocessed_cpu_topic'
memory_consumer_topic = 'collected_memory_topic' 
memory_consumer_group = 'collected_memory_group'
memory_producer_topic = 'preprocessed_memory_topic'
dict_global_max = {}
dict_global_min = {}
global_max = 0
global_min = 0
saved_decoded_batches = []
deployment_state_topic="deployment_state_topic"
batch_size_increment_step=100
stability_factor = 0.02
END_TO_END_TIME_THRESHOLD = 3.0
timeout_ms = 100
eNDBF = "" 
current_batch_size=0, 
new_batch_size=0, 
deployment_stability=False
inference_data_portion = 0.0

# Create Kafka consumer and producer



def stabilise_deployment():

	
	global deployment_stability
	consumer = KafkaConsumer(deployment_state_topic, bootstrap_servers=eNDBF, enable_auto_commit=False, auto_offset_reset='earliest')
	while True:
		
		if eNDBF == "" or current_batch_size==0:
			print("Empty input value eNDBF:",eNDBF," current_batch_size:",current_batch_size)
			continue
		
		records = consumer.poll(timeout_ms=timeout_ms)
		#print("\tstabilise_deployment ",records)
		if len(records) == 0: 
			continue
		
		json_data = {}
		for tp, items in records.items():
			for item in items: 
				data = item.value.decode('utf-8')
				json_data = json.loads(data)
		print("\tConsider last received deployment state data ",json_data)                
		preprocessing_time = float(json_data.get("preprocessing_time"))
		training_time = float(json_data.get("training_time"))
		if training_time == 0:
			training_time = preprocessing_time
			
		end_to_end_time = float(json_data.get("end_to_end_time"))
		deployment_stability = False
		if ((1 + stability_factor) * (1/(preprocessing_time)) < 1/training_time) & (end_to_end_time < END_TO_END_TIME_THRESHOLD):
				deployment_stability = True
		
		if deployment_stability == True:
			continue
		
		global new_batch_size
		if end_to_end_time < END_TO_END_TIME_THRESHOLD:
			new_batch_size = current_batch_size + batch_size_increment_step
		else:
			if current_batch_size - batch_size_increment_step > 0:
				new_batch_size = current_batch_size - batch_size_increment_step
			

	
def get_ntp_server_time():
	
	wait_for_response = True
	retry = 0
	while wait_for_response:
		try:
			ntp_client = ntplib.NTPClient()
			response = ntp_client.request('ntp.cnam.fr')
			wait_for_response = False
		except (ntplib.NTPException) as e:
			print('NTP client request error:', str(e))
			retry = retry + 1
			time.sleep(1)
		if retry == 3:
			wait_for_response = False
	return response.tx_timestamp
			
			
	
	
# Function to decode the message value
def decode_message(batches,batch_size):
		
	decoded_batches_training = []
	decoded_batches_inference = []
	
	#We need to take 20% of data set for inference
	inference_data_size = batch_size * inference_data_portion
	count = 0
	for record in batches:
		# Decode the message value
		data = record.value.decode('utf-8')
		count = count + 1
		if count < inference_data_size:
			decoded_batches_inference.append(data)
		else:
			decoded_batches_training.append(data)
	return decoded_batches_training,decoded_batches_inference

def expand_decoded_batch(current_decoded_batches, batch_size):
	
	count_record = 0
	valid_decoded_batch = []
	for record in current_decoded_batches:
		saved_decoded_batches.append(record)
	
	if 	len(saved_decoded_batches) >= batch_size:
		copy_saved_decoded_batches = saved_decoded_batches.copy()
		for record in copy_saved_decoded_batches:
			valid_decoded_batch.append(record)
			saved_decoded_batches.remove(record)
			count_record = count_record + 1
			if count_record == batch_size:
				break
	
	return valid_decoded_batch
			
	
	
# Function to consume messages in batches with out polling 
def consume_batches_preprocess(resource_name, initial_batch_size, consumer_topic, producer_topic_training, producer_topic_inference, iNDBF, eNDBF_):
	

	global eNDBF
	eNDBF = eNDBF_
	consumer = KafkaConsumer(consumer_topic, bootstrap_servers=iNDBF, enable_auto_commit=False, auto_offset_reset='earliest')
	
	print("Consuming messages...")
	message_count = 0
	batch = []
	count_batch = 1
	timestamp_polling_start = 0
	batch_size = initial_batch_size
	global current_batch_size
	current_batch_size = initial_batch_size
	global new_batch_size
	new_batch_size = initial_batch_size
	global deployment_stability
	deployment_stability = False
	start_stabilise_deployment = False
	previous_batch_size = initial_batch_size
	
	for message in consumer:
		
		
		if new_batch_size !=  current_batch_size:
			previous_batch_size = current_batch_size
			batch_size=new_batch_size
			current_batch_size=new_batch_size
		
		if message_count == 0:
			timestamp_polling_start = get_ntp_server_time()
				
		batch.append(message)
		message_count += 1
		#print(message_count)

		if message_count == batch_size:
			
			start_time = get_ntp_server_time()
			timestamp_prep_start = str(start_time)
			# Decode and preprocess the batch of messages
			decoded_batches_training, decoded_batches_inference = decode_message(batch,batch_size)
			sequence_dict_training_data = {}
			sequence_dict_inference_data = {}
			
			if len(decoded_batches_inference) == 0:	
				
				sequence_dict_training_data = preprocess(decoded_batches_training, resource_name, timestamp_prep_start,timestamp_polling_start)
				sequence_dict_inference_data = sequence_dict_training_data.copy()
				
			else:
				
				sequence_dict_training_data = preprocess(decoded_batches_training, resource_name, timestamp_prep_start,timestamp_polling_start)
				sequence_dict_inference_data = preprocess(decoded_batches_inference, resource_name, timestamp_prep_start,timestamp_polling_start)
				
			send_to_egress_NDBF(sequence_dict_training_data,eNDBF, producer_topic_training)
			send_to_egress_NDBF(sequence_dict_inference_data,eNDBF, producer_topic_inference)
			delta = get_ntp_server_time() - start_time
			print('Receive ',message_count,' new network data samples')
			print('Total number of batches: ',count_batch)
			print('Pre-processing time:', delta, 'sec')
			print("deployment_stability:",deployment_stability)
			print("previous_batch_size:",previous_batch_size," current_batch_size:",batch_size,"new_batch_size:",new_batch_size)
			print('----------------------------------Sending----------------------------------\n')
			save_meta_data_csv(count_batch, message_count,batch_size)
			count_batch += 1
			# Reset message count and the batch
			message_count = 0
			batch = []
			'''if (start_stabilise_deployment == False) & (count_batch > 1):
				print('Start start_stabilise_deployment', start_stabilise_deployment)
				start_stabilise_deployment = True
				thread = Thread(target=stabilise_deployment)
				thread.start()
			'''	
				
			
def send_to_egress_NDBF(sequences_dict,eNDBF,producer_topic):
	
	sequences_dict_converted = convert_ndarray_to_list(sequences_dict)
	json_value = json.dumps(sequences_dict_converted)
	producer = KafkaProducer(bootstrap_servers=eNDBF, api_version=(0, 10))
	producer.send(producer_topic, value=json_value.encode('utf-8'))			
	

# Function to consume messages in batches with polling 
def polling_batches_preprocess(resource_name, batch_size, consumer_topic, producer_topic, iNDBF, eNDBF):
	
	consumer = KafkaConsumer(consumer_topic, bootstrap_servers=iNDBF, enable_auto_commit=False, auto_offset_reset='earliest',group_id=cpu_consumer_group,max_poll_records=batch_size)
	producer = KafkaProducer(bootstrap_servers=eNDBF, api_version=(0, 10))
	print("Consuming messages...")
	batch_count = 0
	message_count = 0
	while True:
		
		batch = consumer.poll(timeout_ms=timeout_ms)
		
		if batch is None: 
			continue
		
		# Decode and preprocess the batch of messages
		batch_decoded = decode_message(batch, True)
		
		#batch_decoded = expand_decoded_batch(batch_decoded, batch_size)
		batch_length = len(batch_decoded)
		if batch_length == 0:
			print("Waiting...")
			continue
		
		
		batch_count = batch_count + 1
		message_count = message_count + 1
		start_time = get_ntp_server_time()
		timestamp_prep_start = str(start_time)
		
		sequences_dict = preprocess(batch_decoded, resource_name, timestamp_prep_start)

		# Publish to Kafka topic
		sequences_dict_converted = convert_ndarray_to_list(sequences_dict)
		json_value = json.dumps(sequences_dict_converted)
		delta = get_ntp_server_time() - start_time
		producer.send(producer_topic, value=json_value.encode('utf-8'))
		save_meta_data_csv(batch_count,len(batch_decoded),batch_size)
		print('Receive ',batch_length,' new network data samples')
		print('Total number of batches: ',batch_count)
		print('Pre-processing time:', delta, 'sec')
		print('--------------------------------------------------')


# Function to create sequences; Receives a 3D array normalized values and returns a 3D array of sequences
def create_sequences(values, lookback=2):
	output = []
	for i in range(len(values) - lookback + 1):
		output.append(values[i: (i + lookback)])
	return np.stack(output)

# Function to concatenate feautures into a single array per timestamtimestamp_prep_startp_col (integer so we have more values per time window (1 second))

def extract_and_normalize_metrics(batch_list,resource_name,timestamp_prep_start):
	
	features = 0
	output_data = {}
	normalized_batch_list_in_json, earliest_publish_time, earliest_collection_time = incremental_min_max_normalization(batch_list)
	
	for json_data in normalized_batch_list_in_json:
		#json_data = json.loads(data)
		source_id = json_data['source_id']
		#timestamp_col = json_data['timestamp_col']
		date_format = '%Y-%m-%d %H:%M:%S.%f'	   
		timestamp_train = int(float(timestamp_prep_start))
		#timestamp_pub = json_data['timestamp_pub']
			
		metrics = {key: value for key, value in json_data.items() if not key.startswith('timestamp') and not key.startswith('source_id')}
		features = len(metrics.values())
		
		if timestamp_train not in output_data:
			output_data[timestamp_train] = {
				'metrics': [],
				'source_id': source_id,
				'timestamp_col': earliest_collection_time,
				'timestamp_pub': earliest_publish_time
			}

		output_data[timestamp_train]['metrics'].extend(list(metrics.values()))

	return output_data, features


# Function to incrementally min-max normalize the data
def incremental_min_max_normalization(batch_list):
	
	
	keys = []
	
	#Set  the local min and the local max from the new batch	
	earliest_publish_time = 0	
	for data in batch_list:
		json_data = json.loads(data)
		
		if earliest_publish_time == 0:
			earliest_publish_time = float(json_data['timestamp_pub'])
			earliest_collection_time = json_data['timestamp_col']
		elif float(json_data['timestamp_pub']) < earliest_publish_time:
			earliest_publish_time = float(json_data['timestamp_pub'])
			earliest_collection_time = json_data['timestamp_col']
			
		for key, value in json_data.items():
			
		##Set now the global min and the global max		
			if not key.startswith('timestamp') and not key.startswith('source_id'):
				keys.append(key)
				if key in dict_global_max:
					if json_data[key] > dict_global_max[key]:
						dict_global_max[key] = json_data[key]
				else:
					dict_global_max[key] = json_data[key]
				
				
				if key in dict_global_min:
					if json_data[key] < dict_global_min[key]:
						dict_global_min[key] = json_data[key]
				else:
					dict_global_min[key] = json_data[key]

	normalized_json_data_records = []
	for data in batch_list:
		json_data = json.loads(data)
		normalised_json_data = json_data.copy()
		for key in keys:
			range_value = dict_global_max[key] - dict_global_min[key]
			if dict_global_max[key] == dict_global_min[key]:
				range_value = 1
				
			normalized_value =  (json_data[key] - dict_global_min[key]) / range_value			
			normalised_json_data[key] = normalized_value			
		normalized_json_data_records.append(normalised_json_data)
	return normalized_json_data_records, earliest_publish_time, earliest_collection_time
	
# Function to ajdust features
def adjust_features(feature_batch_dict):
	
	normalized_features_dict = {}

	for timestamp_train, feature_batch_array in feature_batch_dict.items():
		metrics_array = feature_batch_array['metrics']	
		source_id = feature_batch_array['source_id']
		timestamp_col = feature_batch_array['timestamp_col']
		timestamp_pub = feature_batch_array['timestamp_pub']
		
		if timestamp_train not in normalized_features_dict:
			normalized_features_dict[timestamp_train] = {
				'metrics': [],
				'source_id': source_id,
				'timestamp_col': timestamp_col,
				'timestamp_pub': timestamp_pub
			}

		normalized_features_dict[timestamp_train]['metrics'].extend(metrics_array)

	return normalized_features_dict

def preprocess(batch_list,resource_name,timestamp_prep_start,timestamp_polling_start):
	sequence_dict = {}
	batch_dict, features = extract_and_normalize_metrics(batch_list,resource_name,timestamp_prep_start)
	lookback = 2
	
	#print('features',features)
	norm_batch_dict = adjust_features(batch_dict)
	#print('norm_batch_dict',norm_batch_dict)
	
	for timestamp_train, norm_batch_array in norm_batch_dict.items():
		metrics_norm_batch_array = norm_batch_array['metrics'] 
		source_id = norm_batch_array['source_id']
		timestamp_col = norm_batch_array['timestamp_col']
		timestamp_pub = norm_batch_array['timestamp_pub']
		
		norm_batch_2d = np.array(metrics_norm_batch_array).reshape(-1, features)
		#print('norm_batch_2d.shape',norm_batch_2d.shape)
		
		if norm_batch_2d.shape[0] > 1:
			sequence = create_sequences(norm_batch_2d)
			
			if timestamp_train not in sequence_dict:
				sequence_dict[timestamp_train] = {
					'metrics': [],
					'source_id': source_id,
					'timestamp_col': timestamp_col,
					'timestamp_pub': timestamp_pub,
					'timestamp_prep_start': timestamp_prep_start,
					'timestamp_polling_start': timestamp_polling_start,
					'timestamp_prep_end': str(get_ntp_server_time())
				}

			sequence_dict[timestamp_train]['metrics'].extend(sequence)

	return sequence_dict

def save_meta_data_csv(count_batch, message_count,batch_size):
	
	file_name = 'meta_data_'+str(batch_size)+'.csv'
	file_exists = False
	try:
		with open(file_name, 'r') as csvfile:
			reader = csv.reader(csvfile)
			if any(row for row in reader):
				file_exists = True
	except FileNotFoundError:
		pass
	
	with open(file_name, 'a', newline='') as csvfile:
		writer = csv.writer(csvfile)
		if not file_exists:
			writer.writerow(['num_batch, message_number'])
		writer.writerow([count_batch,message_count])


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
	if len(sys.argv) < 9:
		print("Usage: python3 kafka_preprocess.py <policy: polling or no_polling> <resource_name> <batch_size> <consumer_topic> <producer_topic_traing> <producer_topic_inference> <iNDBF_server:port_number> <eNDBF_server:port_number>")
		print("Usage example: python3 ndppf.py polling cpu 100 collected_cpu_topic preprocessed_cpu_topic2 preprocessed_cpu_topic2 163.173.170.89:9092 163.173.170.89:9092")
		sys.exit(1)
	policy = str(sys.argv[1])
	resource_name = sys.argv[2]
	batch_size = int(sys.argv[3])
	consumer_topic = sys.argv[4]
	producer_topic1 = sys.argv[5]
	producer_topic2 = sys.argv[6]
	iNDBF = sys.argv[7]
	eNDBF = sys.argv[8]

	# Consume messages in batches and preprocess
	consume_batches_preprocess(resource_name, batch_size,consumer_topic, producer_topic1,producer_topic2, iNDBF, eNDBF)
		