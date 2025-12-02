import docker
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

def debug(msg):
    if config["debug"]:
        timestamp = datetime.now().strftime("%s:%f")
        print(f"[{timestamp}] \033[32m{msg}\033[0m")

def stop_and_remove_container(container):
    debug(f"Stopping and removing container: {container.name}")
    container.stop()
    container.remove()

def cleanup_cassandra_cluster():
    client = docker.from_env()
    
    # Define the network and container name patterns
    network_name = 'cassandra-network'
    container_name_prefix = config["node_name"]
    
    # Stop and remove all Cassandra containers
    containers = client.containers.list(all=True, filters={"name": container_name_prefix})
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(stop_and_remove_container, container) for container in containers]
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as e:
                print(f"Error: {e}")
        
    debug("Cleanup completed.")

if __name__ == "__main__":

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

    cleanup_cassandra_cluster()
