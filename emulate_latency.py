import docker
import sys
import time
import math
import re
import csv
import os
from datetime import datetime

import infra

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
        
def load_measured_rtts():
    """Measured round-trip times (ms) of the active provider, keyed by the
    unordered pair of location names, from infra/<provider>/latencies.csv
    (src,dst,rtt_ms; lines starting with '#' are comments).  The two
    directions of a pair are averaged.  Empty if the provider has no such
    file, in which case latencies are estimated from distances."""
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                        "infra", infra.provider(), "latencies.csv")
    if not os.path.exists(path):
        return {}
    samples = {}
    with open(path, newline='') as f:
        rows = (line for line in f if not line.lstrip().startswith('#'))
        for row in csv.DictReader(rows):
            pair = frozenset((row['src'].strip(), row['dst'].strip()))
            samples.setdefault(pair, []).append(float(row['rtt_ms']))
    return {pair: sum(v) / len(v) for pair, v in samples.items()}

def one_way_latency(loc1, loc2, measured_rtts):
    """One-way delay (ms) between two (lat, lon, name) locations: half their
    measured round-trip time, or the great-circle estimate if the provider has
    no measurements at all.  A provider with measurements must cover every
    pair of its locations."""
    if not measured_rtts:
        return estimate_latency(haversine(loc1[0], loc1[1], loc2[0], loc2[1]))
    rtt = measured_rtts.get(frozenset((loc1[2], loc2[2])))
    if rtt is None:
        raise KeyError(f"no measured latency between {loc1[2]} and {loc2[2]} in "
                       f"infra/{infra.provider()}/latencies.csv")
    return round(rtt / 2, 2)

def compute_optimum_per_replica(locations, n_nodes):
    """For each of the first n_nodes (lat, lon, name) locations, the RTT (ms)
    to its closest majority quorum: the local replica plus the (quorum - 1)
    nearest others, so the RTT to the farthest of these.  Uses the same
    latencies as the emulation (see one_way_latency)."""
    measured_rtts = load_measured_rtts()
    quorum = (n_nodes // 2) + 1
    optimums = []
    for i in range(n_nodes):
        rtts = sorted(2 * one_way_latency(locations[i], locations[j], measured_rtts)
                      for j in range(n_nodes) if j != i)
        if not rtts:
            # single-node case -> no neighbour
            optimums.append(0.0)
            continue
        optimums.append(rtts[max(1, quorum - 1) - 1])
    return optimums

def run_tc(container, cmd):
    res = container.exec_run(cmd)
    if res.exit_code != 0:
        if "qdisc del" in cmd:
            return
        raise Exception(f"tc command failed (exit {res.exit_code}): {cmd}\nOutput: {res.output.decode('utf-8')}")

def emulate_latency(num_dcs, nodes_per_dc, dc_locations):
    if not config.get("latency_simulation", 1):
        return
            
    network_name = config["network_name"]
    measured_rtts = load_measured_rtts()

    containers_info = []
    global_idx = 0
    for i in range(num_dcs):
        _, _, dc_name = dc_locations[i]
        for k in range(1, nodes_per_dc + 1):
            container_name = f"{dc_name}{k}"
            try:
                infra.container_for(container_name)
            except docker.errors.NotFound:
                alt_name = f"database-node{i + 1}"
                try:
                    infra.container_for(alt_name)
                    container_name = alt_name
                except docker.errors.NotFound:
                    debug(f"Container '{container_name}' / '{alt_name}' not found. Skipping.")
                    continue
            containers_info.append((i, k, container_name, global_idx))
            global_idx += 1

    total_containers = len(containers_info)
    if total_containers == 0:
        debug("No active database containers found for latency emulation.")
        return

    try:
        priomap = " ".join(["0"] * 16)
        for _, _, cname, _ in containers_info:
            c = infra.container_for(cname)
            dev = infra.net_device(cname)
            if c.exec_run("tc -help").exit_code != 0:
                raise Exception(f"tc is missing in container {cname}!")
            run_tc(c, f"tc qdisc del dev {dev} root")
            run_tc(c, f"tc qdisc add dev {dev} root handle 1: prio bands {total_containers + 1} priomap {priomap}")

        # Add the latency between each pair of DCs (see one_way_latency)
        for idx1 in range(total_containers):
            dc1, k1, src_name, _ = containers_info[idx1]
            src_container = infra.container_for(src_name)
            src_dev = infra.net_device(src_name)

            for idx2 in range(idx1 + 1, total_containers):
                dc2, k2, dst_name, _ = containers_info[idx2]
                if dc1 == dc2:
                    # Intra-DC latency is negligible (~0ms, no tc rule needed)
                    continue

                latency = one_way_latency(dc_locations[dc1], dc_locations[dc2], measured_rtts)
                dst_container = infra.container_for(dst_name)
                dst_dev = infra.net_device(dst_name)

                dst_ip = dst_container.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']
                src_ip = src_container.attrs['NetworkSettings']['Networks'][network_name]['IPAddress']

                dst_band = idx2 + 2
                src_band = idx1 + 2

                # Add latency from src to dst
                run_tc(src_container, f"tc qdisc add dev {src_dev} parent 1:{dst_band:x} handle {dst_band:x}0: netem delay {latency}ms")
                run_tc(src_container, f"tc filter add dev {src_dev} protocol ip parent 1:0 prio 1 u32 match ip dst {dst_ip} flowid 1:{dst_band:x}")

                # Add latency from dst to src
                run_tc(dst_container, f"tc qdisc add dev {dst_dev} parent 1:{src_band:x} handle {src_band:x}0: netem delay {latency}ms")
                run_tc(dst_container, f"tc filter add dev {dst_dev} protocol ip parent 1:0 prio 1 u32 match ip dst {src_ip} flowid 1:{src_band:x}")

                debug(f"Added {latency}ms one-way latency between '{src_name}' and '{dst_name}'.")

    except Exception as e:
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
    with open(infra.locations_file(), newline='') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            lat = float(row['lat'])
            lon = float(row['lon'])
            loc = row['loc'].strip().strip('"')
            locations.append((lat, lon, loc))

    dc_locations = locations[:num_dcs]
    emulate_latency(num_dcs, nodes_per_dc, dc_locations)
