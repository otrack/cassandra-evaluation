import os
import sys

import docker
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import infra

def debug(msg):
    if config["debug"]:
        timestamp = datetime.now().strftime("%s:%f")
        print(f"[{timestamp}] \033[32m{msg}\033[0m")

def stop_and_remove_container(container):
    debug(f"Stopping and removing container: {container.name}")
    try:
        container.stop(timeout=10)
    except Exception:
        pass
    try:
        container.remove()
    except Exception:
        pass

def cleanup_cassandra_cluster():
    cities = []
    try:
        import csv
        with open(infra.locations_file(), newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                cities.append(row['loc'].strip().strip('"'))
    except Exception:
        pass

    container_name_prefix = config.get("node_name", "database-node")
    # Spans every daemon of the deployment: one locally, one per machine when a
    # real provider is in use.
    all_containers = infra.all_containers(all=True)
    containers = []

    for c in all_containers:
        name = c.name
        matches_city = any(name.startswith(city) for city in cities) if cities else False
        matches_prefix = name.startswith(container_name_prefix)
        matches_image = any(img in c.image.tags for img in [config.get("normal_cassandra_image", ""), config.get("accord_cassandra_image", "")] if img)
        if matches_city or matches_prefix or matches_image:
            containers.append(c)

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
