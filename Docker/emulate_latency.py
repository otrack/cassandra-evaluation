import docker
import sys
import time
import math
import re
import csv
from datetime import datetime

def debug(msg):
    if config.get("debug", 1):
        timestamp = datetime.now().strftime("%s:%f")
        print(f"[{timestamp}] \033[32m{msg}\033[0m")
        
def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in kilometers
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon1 - lon2)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def estimate_latency(distance_km):
    speed_of_light_km_per_ms = 204  # km/ms
    latency_ms = distance_km / speed_of_light_km_per_ms
    return math.floor(latency_ms)
        
def run_tc(container, cmd):
    res = container.exec_run(cmd)
    if res.exit_code != 0:
        if "qdisc del" in cmd:
            return
        raise Exception(f"tc command failed (exit {res.exit_code}): {cmd}\nOutput: {res.output.decode('utf-8')}")

def emulate_latency(num_dcs, nodes_per_dc, dc_locations):
    if not config.get("latency_simulation", 1):
        return
            
    client = docker.from_env()
    network_name = config["network_name"]

    # Build container list: (dc_index, node_in_dc_1indexed, container_name, global_index)
    containers_info = []
    global_idx = 0
    for i in range(num_dcs):
        _, _, city = dc_locations[i]
        for k in range(1, nodes_per_dc + 1):
            container_name = f"{city}{k}"
            containers_info.append((i, k, container_name, global_idx))
            global_idx += 1

    total_containers = len(containers_info)

    try:
        priomap = " ".join(["0"] * 16)
        for _, _, cname, _ in containers_info:
            c = client.containers.get(cname)
            if c.exec_run("tc -help").exit_code != 0:
                raise Exception(f"tc is missing in container {cname}!")
            run_tc(c, "tc qdisc del dev eth0 root")
            run_tc(c, f"tc qdisc add dev eth0 root handle 1: prio bands {total_containers + 1} priomap {priomap}")

        # Add specific latencies based on geographical distances between different DCs
        for idx1 in range(total_containers):
            dc1, k1, src_name, _ = containers_info[idx1]
            lat1, lon1, _ = dc_locations[dc1]
            src_container = client.containers.get(src_name)

            for idx2 in range(idx1 + 1, total_containers):
                dc2, k2, dst_name, _ = containers_info[idx2]
                if dc1 == dc2:
                    # Intra-DC latency is negligible (~0ms, no tc rule needed)
                    continue

                lat2, lon2, _ = dc_locations[dc2]
                distance = haversine(lat1, lon1, lat2, lon2)
                latency = estimate_latency(distance)
                dst_container = client.containers.get(dst_name)

                dst_ip = dst_container.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']
                src_ip = src_container.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']

                dst_band = idx2 + 2
                src_band = idx1 + 2

                # Add latency from src to dst
                run_tc(src_container, f"tc qdisc add dev eth0 parent 1:{dst_band} handle {dst_band}0: netem delay {latency}ms")
                run_tc(src_container, f"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {dst_ip} flowid 1:{dst_band}")

                # Add latency from dst to src
                run_tc(dst_container, f"tc qdisc add dev eth0 parent 1:{src_band} handle {src_band}0: netem delay {latency}ms")
                run_tc(dst_container, f"tc filter add dev eth0 protocol ip parent 1:0 prio 1 u32 match ip dst {src_ip} flowid 1:{src_band}")

                debug(f"Added {latency}ms ping latency between '{src_name}' and '{dst_name}' (distance: {distance:.2f} km).")

    except docker.errors.APIError as e:
        print(f"Error adding latency: {e}")

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 emulate_latency.py <num_dcs> [nodes_per_dc]")
        sys.exit(1)

    config = {}
    with open('exp.config', 'r') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith('#'):
                continue
            if '=' in line:
                key, value = line.split('=', 1)
                value = value.strip()
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass
                config[key.strip()] = value

    num_dcs = int(sys.argv[1])
    nodes_per_dc = int(sys.argv[2]) if len(sys.argv) > 2 else int(config.get("nodesperdc", 1))

    locations = []
    with open('latencies.csv', newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            lat = float(row['lat'])
            lon = float(row['lon'])
            loc = row['loc'].strip().strip('"')
            locations.append((lat, lon, loc))

    dc_locations = locations[:num_dcs]
    emulate_latency(num_dcs, nodes_per_dc, dc_locations)
