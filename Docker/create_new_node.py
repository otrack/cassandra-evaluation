import docker
import sys
from start_cassandra_cluster import locations_lat_long, LATENCY_SIMULATION, NORMAL_CASSANDRA_IMAGE, ACCORD_CASSANDRA_IMAGE, haversine, estimate_latency, wait_for_log

def get_existing_nodes(client):
    containers = client.containers.list(filters={"name": "cassandra-node"})
    node_names = [container.name for container in containers]
    return node_names

def get_max_node_number(node_names):
    max_number = 0
    for name in node_names:
        try:
            number = int(name.replace("cassandra-node", ""))
            if number > max_number:
                max_number = number
        except ValueError:
            continue
    return max_number

def add_latency_between_nodes(client, new_node_name, node_names, node_locations):
    network_name = 'cassandra-network'
    new_node = client.containers.get(new_node_name)
    new_node_ip = new_node.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']
    new_node_index = len(node_names)  # New node index in locations list

    for i, node_name in enumerate(node_names):
        existing_node = client.containers.get(node_name)
        existing_node_ip = existing_node.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']
        
        lat1, lon1 = node_locations[i]
        lat2, lon2 = node_locations[new_node_index]
        distance = haversine(lat1, lon1, lat2, lon2)
        latency = estimate_latency(distance)
        
        try:
            # Add latency from new node to existing node
            exec_command = f"tc qdisc add dev eth0 root netem delay {latency}ms"
            new_node.exec_run(exec_command)
            exec_command = f"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {existing_node_ip} flowid 1:1"
            new_node.exec_run(exec_command)
            
            # Add latency from existing node to new node
            exec_command = f"tc qdisc add dev eth0 root netem delay {latency}ms"
            existing_node.exec_run(exec_command)
            exec_command = f"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {new_node_ip} flowid 1:1"
            existing_node.exec_run(exec_command)
            
            print(f"Added {latency:.2f}ms latency between '{new_node_name}' and '{node_name}' (distance: {distance:.2f} km).")
        except docker.errors.APIError as e:
            print(f"Error adding latency between '{new_node_name}' and '{node_name}': {e}")

def create_new_node(cassandra_image):
    client = docker.from_env()
    log_pattern = r"CassandraDaemon.java:745 - Startup complete"
    node_names = get_existing_nodes(client)
    max_node_number = get_max_node_number(node_names)
    new_node_number = max_node_number + 1
    new_node_name = f"cassandra-node{new_node_number}"

    network_name = 'cassandra-network'
    try:
        client.networks.get(network_name)
    except docker.errors.NotFound:
        print(f"Network '{network_name}' does not exist. Please start the cluster first.")
        sys.exit(1)

    try:
        container = client.containers.run(
            image=cassandra_image,
            name=new_node_name,
            network=network_name,
            environment={
                "CASSANDRA_SEEDS": "cassandra-node1",
                "CASSANDRA_CLUSTER_NAME": "TestCluster",
                "CASSANDRA_DC": "DC1",
                "CASSANDRA_RACK": "RAC1"
            },
            cap_add=["NET_ADMIN"],  # Add NET_ADMIN capability
            detach=True
        )
        print(f"Started new container '{new_node_name}'.")
        if not wait_for_log(container, log_pattern):
            print(f"Failed to start container '{new_node_name}' within the timeout period.")
            return
        
        if LATENCY_SIMULATION:
            node_locations = locations_lat_long[:new_node_number]
            add_latency_between_nodes(client, new_node_name, node_names, node_locations)
    except docker.errors.APIError as e:
        print(f"Error starting container '{new_node_name}': {e}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 create_new_node.py <cassandra_image>")
        sys.exit(1)

    cassandra_image_version = sys.argv[1]
    if cassandra_image_version != "normal" and cassandra_image_version != "accord":
        print("Cassandra image must be either 'normal' or 'accord'")
        sys.exit(1)

    cassandra_image = NORMAL_CASSANDRA_IMAGE if cassandra_image_version == "normal" else ACCORD_CASSANDRA_IMAGE
    create_new_node(cassandra_image)
