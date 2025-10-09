import docker
import sys
import time
import math
import re
from datetime import datetime

locations_lat_long = [
    (21.027763, 105.834160), # Hanoi
    (45.764042, 4.835659), # Lyon
    (40.712776, -74.005974), # New York
    (39.904202, 116.407394), # Beijing
    (19.075983, 72.877655), # Mumbai
    (51.924419, 4.477733), # Rotterdam
    (31.968599, -99.901810), # Texas
    (-23.550520, -46.633308), # Sao Paulo
    (51.507351, -0.127758), # London
    (1.352083, 103.819839), # Singapore
    (35.689487, 139.691711), # Tokyo
    (32.776665, -96.796989) # Dallas
]

def debug(msg):
    if config["debug"]:
        timestamp = datetime.now().strftime("%s:%f")
        print(f"[{timestamp}] \033[32m{msg}\033[0m")
        
def haversine(lat1, lon1, lat2, lon2):
    # Calculate the great-circle distance between two points on the Earth
    R = 6371  # Earth radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon1 - lon2)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def estimate_latency(distance_km):
    # return 50
    # Speed of light in fiber optics is approximately 200,000 km/s
    speed_of_light_km_per_ms = 200  # km/ms
    latency_ms = distance_km / speed_of_light_km_per_ms
    return math.floor(latency_ms)

def wait_for_log(container, log_pattern, timeout=300):
    log_stream = container.logs(stream=True)
    start_time = time.time()
    for log in log_stream:
        if re.search(log_pattern, log.decode('utf-8')):
            debug(f"Log pattern '{log_pattern}' found in container '{container.name}'.")
            return True
        if time.time() - start_time > timeout:
            debug(f"Timeout waiting for log pattern '{log_pattern}' in container '{container.name}'.")
            return False
    return False

def create_cassandra_cluster(num_nodes, cassandra_image, node_locations):
    client = docker.from_env()
    
    # Create a Docker network if it doesn't exist
    network_name = 'cassandra-network'
    try:
        client.networks.get(network_name)
        debug(f"Network '{network_name}' already exists.")
    except docker.errors.NotFound:
        client.networks.create(network_name, driver="bridge")
        debug(f"Created network '{network_name}'.")

    # Start the Cassandra nodes
    containers = []
    log_pattern = r"Startup complete"
    for i in range(1, num_nodes + 1):
        container_name = f'cassandra-node{i}'
        dc_name = f'DC{i}'
        try:
            container = client.containers.run(
                image=cassandra_image,
                name=container_name,
                network=network_name,
                auto_remove=True,
                mem_limit=config["xmx"],
                environment={
                    "JVM_OPTS" : " -Xms2g -Xmx"+config["xmx"], 
                    "CASSANDRA_SEEDS": "cassandra-node1" if i > 1 else "",
                    "CASSANDRA_CLUSTER_NAME": "TestCluster",
                    "CASSANDRA_DC": dc_name,
                    "CASSANDRA_RACK": "RAC1"
                },
                cap_add=["NET_ADMIN"],  # Add NET_ADMIN capability,
                ports={ '9042/tcp': ('127.0.0.1', (3333+i)), '5005/tcp': ('127.0.0.1', (5005+i)) },
                detach=True
            )
            containers.append(container)            
            debug(f"Started container '{container_name}' in data center '{dc_name}'.")
            if not wait_for_log(container, log_pattern):
                debug(f"Failed to start container '{container_name}' within the timeout period.")
                exit(-1)
        except docker.errors.APIError as e:
            debug(f"Error starting container '{container_name}': {e}")

    if config["latency_simulation"]:
        for i in range(num_nodes):
            src = f'cassandra-node{i + 1}'
            src_container = client.containers.get(src)
            exec_command = f"tc qdisc del dev eth0 root"
            debug(f"{src} {exec_command}")
            src_container.exec_run(exec_command)
            exec_command = f"tc qdisc add dev eth0 root handle 1: htb default 30"
            debug(f"{src} {exec_command}")
            src_container.exec_run(exec_command)
            exec_command = f"tc class add dev eth0 parent 1: classid 1:1 htb rate 1000mbit"
            debug(f"{src} {exec_command}")
            src_container.exec_run(exec_command)

        # Add specific latencies based on geographical distances
        for i in range(num_nodes):
            for j in range(i + 1, num_nodes):
                src = f'cassandra-node{i + 1}'
                dst = f'cassandra-node{j + 1}'
                lat1, lon1 = node_locations[i]
                lat2, lon2 = node_locations[j]
                distance = haversine(lat1, lon1, lat2, lon2)
                latency = estimate_latency(distance)
                try:
                    src_container = client.containers.get(src)
                    dst_container = client.containers.get(dst)
                    dst_ip = dst_container.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']
                    src_ip = src_container.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']
                    
                    # Add latency from src to dst
                    exec_command = f"tc class add dev eth0 parent 1:1 classid 1:{j+1}0 htb rate 100mbit"
                    debug(f"{src} {exec_command}")
                    src_container.exec_run(exec_command)
                    exec_command = f"tc qdisc add dev eth0 parent 1:{j+1}0 handle {j+1}0: netem delay {latency}ms"
                    debug(f"{src} {exec_command}")
                    src_container.exec_run(exec_command)
                    exec_command = f"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {dst_ip} flowid 1:{j+1}0"
                    debug(f"{src} {exec_command}")
                    src_container.exec_run(exec_command)
                    
                    # Add latency from dst to src
                    exec_command = f"tc class add dev eth0 parent 1:1 classid 1:{i+1}0 htb rate 100mbit"
                    debug(f"{dst} {exec_command}")
                    dst_container.exec_run(exec_command)
                    exec_command = f"tc qdisc add dev eth0 parent 1:{i+1}0 handle {i+1}0: netem delay {latency}ms"
                    debug(f"{dst} {exec_command}")
                    dst_container.exec_run(exec_command)
                    exec_command = f"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {src_ip} flowid 1:{i+1}0"
                    debug(f"{dst} {exec_command}")
                    dst_container.exec_run(exec_command)


                    latency = 2 * latency
                    debug(f"Added {latency:.2f}ms ping latency between '{src}' and '{dst}' (distance: {distance:.2f} km).")
                except docker.errors.APIError as e:
                    print(f"Error adding latency between '{src}' and '{dst}': {e}")

    debug(f"Started {num_nodes} Cassandra nodes in the cluster with specified latencies.")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 start_cassandra_data_centers.py <num_nodes> <cassandra_image>")
        sys.exit(1)

    try:        
        num_nodes = int(sys.argv[1])
        protocol = sys.argv[2]
        if protocol != "accord" and protocol != "paxos" and protocol != "quorum" and protocol != "one":
            raise ValueError("Protocol must be either 'accord', 'paxos', 'quorum', or 'one' ")
        if num_nodes < 1:
            raise ValueError("Number of nodes must be at least 1.")

        config = {}
        with open('exp.config', 'r') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue  # Skip empty lines and comments
                if '=' in line:
                    key, value = line.split('=', 1)
                    value = value.strip()
                    # Try to cast to int, then float, else keep as string
                    try:
                        value = int(value)
                    except ValueError:
                        try:
                            value = float(value)
                        except ValueError:
                            pass
                    config[key.strip()] = value
        
        cassandra_image = config["accord_cassandra_image"] if protocol == "accord" else config["normal_cassandra_image"]
    except ValueError as e:
        print(f"Invalid number of nodes: {e}")
        sys.exit(1)

    node_locations = locations_lat_long[:num_nodes]

    create_cassandra_cluster(num_nodes, cassandra_image, node_locations)
