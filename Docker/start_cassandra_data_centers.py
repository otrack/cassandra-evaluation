import docker
import sys
import time
import math
import re
from datetime import datetime

def debug(msg):
    if config["debug"]:
        timestamp = datetime.now().strftime("%s:%f")
        print(f"[{timestamp}] \033[32m{msg}\033[0m")

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

def create_cassandra_cluster(num_nodes, cassandra_image):
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
            debug(f"Starting container '{container_name}' in data center '{dc_name}'.")
            if not wait_for_log(container, log_pattern):
                debug(f"Failed to start container '{container_name}' within the timeout period.")
                exit(-1)
        except docker.errors.APIError as e:
            debug(f"Error starting container '{container_name}': {e}")

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

    create_cassandra_cluster(num_nodes, cassandra_image)
