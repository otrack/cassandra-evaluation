import docker
from concurrent.futures import ThreadPoolExecutor, as_completed

def stop_and_remove_container(container):
    print(f"Stopping and removing container: {container.name}")
    container.stop()
    container.remove()

def cleanup_cassandra_cluster():
    client = docker.from_env()
    
    # Define the network and container name patterns
    network_name = 'cassandra-network'
    container_name_prefix = 'cassandra-node'
    
    # Stop and remove all Cassandra containers
    containers = client.containers.list(all=True, filters={"name": container_name_prefix})
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(stop_and_remove_container, container) for container in containers]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {e}")
    
    # Remove the Docker network
    try:
        network = client.networks.get(network_name)
        network.remove()
        print(f"Removed network: {network_name}")
    except docker.errors.NotFound:
        print(f"Network {network_name} not found.")
    
    print("Cleanup completed.")

if __name__ == "__main__":
    cleanup_cassandra_cluster()
