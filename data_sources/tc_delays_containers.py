import subprocess
import json

# Read delay values from the text file
number_of_containers = 20
with open('delays_GoogleYoutube.json', 'r') as json_file:
    json_string = json.load(json_file)
    delays_per_cluster = json.loads(json_string)

# Convert delays from microseconds to milliseconds
clusters = [6,7,8,9]
dict_cluster = {}
dict_cluster['count_for_cluster6']=0
dict_cluster['count_for_cluster7']=0 
dict_cluster['count_for_cluster8']=0 
dict_cluster['count_for_cluster9']=0
dict_cluster['delays_per_cluster6']= list(map(int, delays_per_cluster['cluster6'].split(",")))
dict_cluster['delays_per_cluster7']= list(map(int, delays_per_cluster['cluster7'].split(",")))
dict_cluster['delays_per_cluster8']= list(map(int, delays_per_cluster['cluster8'].split(",")))
dict_cluster['delays_per_cluster9']= list(map(int, delays_per_cluster['cluster9'].split(",")))
 
for i in range(1, number_of_containers+1):
    cluster = clusters[i % len(clusters)]
    index_counter = 'count_for_cluster'+str(cluster)
    index_delays = 'delays_per_cluster'+str(cluster)
    counter = dict_cluster.get(index_counter)
    delay_ms = dict_cluster[index_delays][counter]/1000
    dict_cluster[index_counter] = counter + 1
    container_name = f'data_sources_collect_publish_{i}'
    print(container_name)
    command = f'sudo docker exec -it --privileged {container_name} tc qdisc add dev eth0 root netem delay {delay_ms}ms'
    subprocess.run(command, shell=True, check=True)
    print(f'Delay {delay_ms}ms at index {counter} applied to container {container_name}.')

