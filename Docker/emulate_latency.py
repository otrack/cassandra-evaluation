import docker
import sys
import time
import math
import re
import csv

from datetime import datetime

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
    # Speed of light in fiber optics is approximately 204,000 km/s
    speed_of_light_km_per_ms = 204  # km/ms
    latency_ms = distance_km / speed_of_light_km_per_ms
    return math.floor(latency_ms)
        
def emulate_latency(num_nodes, node_locations):
    if not config["latency_simulation"]:
        return
            
    client = docker.from_env()
    network_name = config["network_name"]

    for i in range(num_nodes):
        src = f'{config["node_name"]}{i + 1}'
        src_container = client.containers.get(src)
        exec_command = f"tc qdisc del dev eth0 root"
        debug(f"{src} {exec_command}")
        src_container.exec_run(exec_command)
        exec_command = f"tc qdisc add dev eth0 root handle 1: htb default 1"
        debug(f"{src} {exec_command}")
        src_container.exec_run(exec_command)
        exec_command = f"tc class add dev eth0 parent 1: classid 1:1 htb rate 1000mbit"
        debug(f"{src} {exec_command}")
        src_container.exec_run(exec_command)

    # Add specific latencies based on geographical distances
    for i in range(num_nodes):
        for j in range(i + 1, num_nodes):
            src = f'{config["node_name"]}{i + 1}'
            dst = f'{config["node_name"]}{j + 1}'
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

    
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python3 emulate_latency.py <num_nodes>")
        sys.exit(1)
        
    try:        
        num_nodes = int(sys.argv[1])
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
    except ValueError as e:
        print(f"Invalid number of nodes: {e}")
        sys.exit(1)

    locations_lat_long = []

    with open('latencies.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # Convert the latitude and longitude to float
            lat = float(row['lat'])
            lon = float(row['lon'])
            locations_lat_long.append((lat, lon))

    node_locations = locations_lat_long[:num_nodes]
        
    emulate_latency(num_nodes, node_locations)
