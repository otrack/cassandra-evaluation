import docker, sys, time, math, re, csv, os
from datetime import datetime

def debug(msg):
    if config.get("debug", 1):
        timestamp = datetime.now().strftime("%s:%f")
        print(f"[{timestamp}] \033[32m{msg}\033[0m")

def read_locations(file_path):
    locations = []
    with open(file_path, 'r') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            locations.append((float(row['lat']), float(row['lon']), row['loc'].strip().strip('"')))
    return locations
        
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

def wait_for_nodetool_status(containers, expected_count, timeout=120):
    start_time = time.time()
    container_list = containers if isinstance(containers, list) else [containers]
    while time.time() - start_time < timeout:
        for c in container_list:
            try:
                res = c.exec_run("env JVM_OPTS='' nodetool status")
                if res.exit_code == 0:
                    output = res.output.decode('utf-8', errors='ignore')
                    un_count = sum(1 for line in output.splitlines() if re.match(r'^\s*UN\b', line))
                    if un_count >= expected_count:
                        debug(f"All {expected_count} Cassandra nodes are UN (Up Normal) in nodetool status (verified via {c.name}).")
                        return True
            except Exception:
                pass
        time.sleep(2)
    debug(f"Timeout waiting for all {expected_count} nodes to become UN.")
    return False

def create_cassandra_cluster(num_dcs, nodes_per_dc, cassandra_image):
    client = docker.from_env()
    network_name = config["network_name"]

    nano_cpus = None
    mem_limit = None
    cassandra_xmx = "4g"
    machine = config.get("machine", "")
    ephemeral_read_enabled = config.get("accord.ephemeral_read_enabled", "true")
    if machine:
        try:
            with open(os.path.join(os.path.dirname(__file__), '..', 'gcp.csv'), 'r') as gcp_file:
                gcp_reader = csv.DictReader(gcp_file)
                for gcp_row in gcp_reader:
                    if gcp_row['name'] == machine:
                        nano_cpus = int(float(gcp_row['vcpus']) * 1e9)
                        memory_gb = float(gcp_row['memory'])
                        mem_limit = int(memory_gb * 1024 * 1024 * 1024 * 4/5)
                        xmx_gb = max(1, math.floor(memory_gb))
                        xms_gb = min(2, xmx_gb)
                        cassandra_xms = f"{xms_gb}g"
                        cassandra_xmx = f"{xmx_gb}g"
                        break
        except FileNotFoundError:
            debug(f"gcp.csv not found, no resource limits applied for machine '{machine}'")

    if 'cassandra_xms' not in locals():
        cassandra_xms = "2g"
        cassandra_xmx = "4g"
    
    containers = []
    log_pattern = r"Startup complete"
    seeds_str = ",".join([f"{locations[idx][2]}1" for idx in range(num_dcs)])

    port_offset = 0
    for i in range(1, num_dcs + 1):
        _, _, dc_name = locations[i-1]
        for k in range(1, nodes_per_dc + 1):
            container_name = f"{dc_name}{k}"
            is_first_node = (i == 1 and k == 1)
            port_offset += 1
            try:
                run_kwargs = dict(
                    image=cassandra_image,
                    name=container_name,
                    network=network_name,
                    auto_remove=True,
                    security_opt=[
                        "seccomp=unconfined",
                        "apparmor=unconfined",
                        "label=disable",
                    ],
                    log_config=docker.types.LogConfig(
                        type="json-file",
                        config={
                            "max-size": "10m",
                            "max-file": "3"
                        }),
                    tmpfs={"/tmp/tmpfs": "rw,nosuid,nodev,mode=1777"},
                    ulimits=[docker.types.Ulimit(name="memlock", soft=-1, hard=-1)],
                    environment={
                        "JVM_EXTRA_OPTS" : " -Xms"+cassandra_xms+" -Xmx"+cassandra_xmx+(" -XX:ActiveProcessorCount="+gcp_row['vcpus'] if machine and 'gcp_row' in locals() else ""),
                        "JVM_OPTS" : " -Xms"+cassandra_xms+" -Xmx"+cassandra_xmx+(" -XX:ActiveProcessorCount="+gcp_row['vcpus'] if machine and 'gcp_row' in locals() else ""),
                        "CASSANDRA_ENDPOINT_SNITCH": "GossipingPropertyFileSnitch",
                        "CASSANDRA_SEEDS": "" if is_first_node else seeds_str,
                        "CASSANDRA_CLUSTER_NAME": "TestCluster",
                        "CASSANDRA_DC": dc_name,
                        "CASSANDRA_RACK": f"RAC{k}",
                        "CASSANDRA_EPHEMERAL_READ_ENABLED": ephemeral_read_enabled
                    },
                    cap_add=["NET_ADMIN"],
                    detach=True
                )
                if nano_cpus is not None:
                    run_kwargs['nano_cpus'] = nano_cpus
                if mem_limit is not None:
                    run_kwargs['mem_limit'] = mem_limit
                container = client.containers.run(**run_kwargs)
                containers.append(container)            
                debug(f"Starting container '{container_name}' in DC '{dc_name}', rack 'RAC{k}'.")
                if not wait_for_log(container, log_pattern):
                    debug(f"Failed to start container '{container_name}' within timeout.")
                    exit(-1)
            except docker.errors.APIError as e:
                debug(f"Error starting container '{container_name}': {e}")

    debug(f"Started {len(containers)} Cassandra nodes across {num_dcs} DCs ({nodes_per_dc} nodes/DC).")
    if containers:
        debug("Waiting for all nodes to be UN in nodetool status...")
        wait_for_nodetool_status(containers, len(containers))

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python3 start_cassandra_data_centers.py <num_dcs> <protocol> [nodes_per_dc]")
        sys.exit(1)

    try:        
        num_dcs = int(sys.argv[1])
        protocol = sys.argv[2]
        if protocol not in ["accord", "paxos", "quorum", "one"]:
            raise ValueError("Protocol must be either 'accord', 'paxos', 'quorum', or 'one'")
        if num_dcs < 1:
            raise ValueError("Number of DCs must be at least 1.")

        latencies_file = os.path.join(os.path.dirname(__file__), '..', 'latencies.csv')
        locations = read_locations(latencies_file)
        
        config = {}
        config_path = os.path.join(os.path.dirname(__file__), '..', 'exp.config')
        with open(config_path, 'r') as f:
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

        nodes_per_dc = int(sys.argv[3]) if len(sys.argv) > 3 else int(config.get("nodesperdc", 1))
        cassandra_image = config["accord_cassandra_image"] if protocol == "accord" else config["normal_cassandra_image"]
    except ValueError as e:
        print(f"Invalid parameters: {e}")
        sys.exit(1)

    create_cassandra_cluster(num_dcs, nodes_per_dc, cassandra_image)
