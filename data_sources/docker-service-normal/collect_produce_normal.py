import psutil
from kafka import KafkaProducer
import time
import json
import uuid

import multiprocessing
import pandas
from scipy.stats import linregress
import random
from joblib import Parallel, delayed
import ntplib


# Generate a unique identifier for the source
source_id = str(uuid.uuid4())[:8]

# Kafka parameters and configuration
###Core network deployment
#bootstrap_servers = "163.173.170.89:9092" #TFX-6
#bootstrap_servers = "163.173.170.88:9092" #TFX-7
#bootstrap_servers = "163.173.170.87:9092" #TFX-8
###Edge deployment
#bootstrap_servers = "172.16.9.148:9092" #TFX-1
#bootstrap_servers = "172.16.9.157:9092" #TFX-9
bootstrap_servers = "163.173.170.89:9092" #TFX-10


#cpu_producer_topic = 'collected_cpu_topic'
#memory_producer_topic = 'collected_memory_topic'
topic_name = "RAW_DATA_TOPIC_NAME"
producer = KafkaProducer(bootstrap_servers=bootstrap_servers, api_version=(0, 10))
#producer=None

sample_counting = 1


# Publish metrics to Kafka
def publish_to_topic(producer_instance, topic_name, metrics_json):
    
    try:
        value_bytes = bytes(metrics_json, encoding='utf-8')
        producer_instance.send(topic_name, value=value_bytes)
        producer_instance.flush()

    except Exception as ex:
        print('Exception in publishing metrics to topic')
        print(str(ex))


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
    


def collect_metrics(source_id):
    metrics = {
        "source_id": source_id,
        "timestamp_col": str(time.time()),
        "cpu_times_user": psutil.cpu_times().user,
        "cpu_times_nice": psutil.cpu_times().nice,
        "cpu_times_system": psutil.cpu_times().system,
        "cpu_times_idle": psutil.cpu_times().idle,
        "cpu_times_iowait": psutil.cpu_times().iowait,
        "cpu_times_irq": psutil.cpu_times().irq,
        "cpu_times_softirq": psutil.cpu_times().softirq,
        "cpu_times_steal": psutil.cpu_times().steal,
        "cpu_times_guest": psutil.cpu_times().guest,
        "cpu_times_guest_nice": psutil.cpu_times().guest_nice,        
        "cpu_utilization": psutil.cpu_percent(),
        "cpu_stats_ctx_switches": psutil.cpu_stats().ctx_switches,
        "cpu_stats_interrupts": psutil.cpu_stats().interrupts,
        "cpu_stats_soft_interrupts": psutil.cpu_stats().soft_interrupts,
        "cpu_stats_syscalls": psutil.cpu_stats().syscalls,
        "memory_usage_total": psutil.virtual_memory().total,
        "memory_usage_available": psutil.virtual_memory().available,
        "memory_usage_percent": psutil.virtual_memory().percent,
        "memory_usage_used": psutil.virtual_memory().used,
        "memory_usage_free": psutil.virtual_memory().free,
        "memory_swap_total": psutil.swap_memory().total,
        "memory_swap_used": psutil.swap_memory().used,
        "memory_swap_free": psutil.swap_memory().free,
        "memory_swap_percent": psutil.swap_memory().percent,
        "memory_swap_sin": psutil.swap_memory().sin,
        "memory_swap_sout": psutil.swap_memory().sout
    }
    return metrics
    
def collect_cpu_metrics(source_id):
    cpu_metrics = {
        "source_id": source_id,
        "timestamp_col": str(time.time()),
        "cpu_times_user": psutil.cpu_times().user,
        "cpu_times_nice": psutil.cpu_times().nice,
        "cpu_times_system": psutil.cpu_times().system,
        "cpu_times_idle": psutil.cpu_times().idle,
        "cpu_times_iowait": psutil.cpu_times().iowait,
        "cpu_times_irq": psutil.cpu_times().irq,
        "cpu_times_softirq": psutil.cpu_times().softirq,
        "cpu_times_steal": psutil.cpu_times().steal,
        "cpu_times_guest": psutil.cpu_times().guest,
        "cpu_times_guest_nice": psutil.cpu_times().guest_nice,        
        "cpu_utilization": psutil.cpu_percent(),
        "cpu_stats_ctx_switches": psutil.cpu_stats().ctx_switches,
        "cpu_stats_interrupts": psutil.cpu_stats().interrupts,
        "cpu_stats_soft_interrupts": psutil.cpu_stats().soft_interrupts,
        "cpu_stats_syscalls": psutil.cpu_stats().syscalls
    }
    return cpu_metrics

def collect_memory_metrics(source_id):
    memory_metrics = {
        "source_id": source_id,
        "timestamp": str(time.time()),
        "memory_usage_total": psutil.virtual_memory().total,
        "memory_usage_available": psutil.virtual_memory().available,
        "memory_usage_percent": psutil.virtual_memory().percent,
        "memory_usage_used": psutil.virtual_memory().used,
        "memory_usage_free": psutil.virtual_memory().free,
        "memory_swap_total": psutil.swap_memory().total,
        "memory_swap_used": psutil.swap_memory().used,
        "memory_swap_free": psutil.swap_memory().free,
        "memory_swap_percent": psutil.swap_memory().percent,
        "memory_swap_sin": psutil.swap_memory().sin,
        "memory_swap_sout": psutil.swap_memory().sout
    }
    return memory_metrics

def collect_publish(producer):
    
    number_of_samples = 1000 #To have approximately 40 minutes of process collecting and publishing  1 sample per 100 ms 
    sampling_rate = 100/1000
    sample_counting = 1
    start = True
    while start:
        try:
            #metrics_cpu_dict = collect_cpu_metrics(source_id)
            #metrics_memory_dict = collect_memory_metrics(source_id)
            
            metrics_dict = collect_metrics(source_id)
            
            # Add timestamp for publishing metrics
            #timestamp_pub = str(time.time())
            timestamp_pub = str(get_ntp_server_time())
            #metrics_cpu_dict["timestamp_pub"] = timestamp_pub
            #metrics_memory_dict["timestamp_pub"] = timestamp_pub
            
            metrics_dict["timestamp_pub"] = timestamp_pub

            # Publish metrics to Kafka
            metrics_json = json.dumps(metrics_dict)
            #print(metrics_json)
            #metrics_memory_json = json.dumps(metrics_memory_dict)
            publish_to_topic(producer, topic, metrics_json)
            #publish_to_topic(producer, memory_producer_topic, metrics_memory_json)
            time.sleep(sampling_rate)
            sample_counting = sample_counting + 1
            if sample_counting == number_of_samples:
                start = False
                
        except KeyboardInterrupt:
            # Handle keyboard interrupt 
            print("Keyboard Interrupt, exiting...")
            break


if __name__ == '__main__':
    
        
    print("Starting kafka metric collection script...")

    # Collect and publish metrics
    print("Collecting and Publishing metrics...") 
    collect_publish(producer)
    print("Stop Collecting and Publishing metrics...")

    # Close the consumer connection
    #producer.close()
