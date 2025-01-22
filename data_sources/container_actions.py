import subprocess
import sys
import datetime

"""kafka_servers = {
    'collect_kafka6': "163.173.170.89:9092",
    'collect_kafka7': "163.173.170.88:9092",
    'collect_kafka8': "163.173.170.87:9092",
    'collect_kafka9': "163.173.170.86:9092"
}"""
#docker_images = ['collect_kafka6','collect_kafka7','collect_kafka8','collect_kafka9']
docker_images = ['collect_kafka-flush-tfx-6-loaded','collect_kafka-flush-tfx-7-loaded','collect_kafka-flush-tfx-8-loaded']
#docker_images = ['collect_kafka-flush-tfx-s145']
clusters = [6,7,8]
def create_containers(num_containers, images):
    """if len(images) < num_containers:
        print("Not enough docker images for the specified number of containers.")
        return"""

    for i in range(1, num_containers + 1):
        cluster = clusters[i % len(clusters)]
        container_name = f"kafka{cluster}_src{i}"
        #kafka_image = list(kafka_servers.keys())[i % len(kafka_servers)
        #kafka_server = kafka_servers[kafka_image]
        docker_image = images[i % len(images)]
        cmd = f"sudo docker run -d --name {container_name} --cap-add=NET_ADMIN {docker_image}"
        #print(cmd)
        subprocess.run(cmd, shell=True)

def stop_containers():
    cmd = "sudo docker stop $(sudo docker ps -a -q)"
    subprocess.run(cmd, shell=True)

def start_containers():
    cmd = "sudo docker start $(sudo docker ps -a -q)"
    subprocess.run(cmd, shell=True)

def remove_containers():
    cmd =  "sudo docker rm $(sudo docker ps --filter status=exited -q)"
    subprocess.run(cmd, shell=True)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 container_actions.py <create, stop, start, or remove>")
        sys.exit(1)
    action = sys.argv[1].strip().lower()
    
    
    start_time = datetime.datetime.now()
    
    if action == "create":
        if len(sys.argv) < 3:
            print("Usage: python3 container_actions.py create number_of_containers")
            sys.exit(1)
        num_containers = int(sys.argv[2])
        create_containers(num_containers,docker_images)

    elif action == "stop":
        stop_containers()

    elif action == "start":
        start_containers()

    elif action == "remove":
        remove_containers()

    else:
        print("Invalid action.")
    
    delta = datetime.datetime.now() - start_time
    print('Duration of the container action[',action,']', delta.total_seconds(), 'sec')
        
