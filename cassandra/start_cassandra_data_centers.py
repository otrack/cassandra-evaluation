import docker, sys, time, math, re, csv, os
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
import infra

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
                res = c.exec_run("nodetool status")
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

def build_extra_hosts(nodes_per_dc):
    """--add-host equivalent of utils.sh:host_aliases(), for real deployments
    where every node owns its own machine and network namespace: containers
    join with --network host instead of a shared bridge, so they lose
    Docker's embedded per-network DNS and need names resolved this way
    instead."""
    aliases = {}
    for _, _, dc_name in locations:
        for k in range(1, nodes_per_dc + 1):
            ip, _ = infra.host_for(f"{dc_name}{k}")
            if ip:
                aliases[f"{dc_name}{k}"] = ip
    return aliases

def profiler_enabled():
    # CASSANDRA_PROFILER in the environment overrides exp.config for one run,
    # as in cassandra/profiler.sh.
    value = os.environ.get("CASSANDRA_PROFILER", config.get("cassandra.profiler", 0))
    return str(value).strip().lower() in ("1", "true")

def profiler_jvm_opts():
    """JVM flags that let cassandra/profiler.sh capture a JFR of the run
    phase, including the spans DebugExecution emits.

    - async_profiler.enabled makes Cassandra load the async-profiler bundled
      in its lib/ (the jar's embedded native library) at startup.  Span's
      natives live in that library, so no separate -agentpath is attached.
      Spans taken before profiling starts are simply 0 and are dropped.
    - async_profiler.unsafe_mode allows `nodetool profile execute`, the only
      entry point that accepts arbitrary async-profiler options.
    - accord.debug_execution creates the DebugExecutor/DebugTask hooks that
      emit the executor critical-section, queued and per-task run spans.
      accord.debug_execution_report additionally logs slow-task/slow-lock
      warnings and keeps latency histograms; it is off unless exp.config asks
      for it, because it reads the thread CPU clock around every task and
      lock hold, partly while the executor lock is held.  The spans do not
      depend on it.
    - DebugNonSafepoints makes async-profiler's stack traces accurate for
      inlined frames.
    """
    if not profiler_enabled():
        return ""
    report = str(config.get("accord.debug_execution_report", "false")).strip().lower()
    return (" -Dcassandra.async_profiler.enabled=true"
            " -Dcassandra.async_profiler.unsafe_mode=true"
            " -Daccord.debug_execution=true"
            f" -Daccord.debug_execution_report={report}"
            " -XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints")

def extra_jvm_opts():
    """Free-form JVM flags for the Cassandra nodes, e.g. the -D switches that
    toggle individual optimisations for A/B runs.  CASSANDRA_JVM_OPTS in the
    environment overrides exp.config for one run."""
    value = os.environ.get("CASSANDRA_JVM_OPTS", config.get("cassandra.jvm_opts", ""))
    value = str(value).strip()
    return " " + value if value else ""

def initial_token(k, nodes_per_dc, dc_index):
    """The single token of node k (from 1) of data center dc_index (from 0)
    with cassandra.fixed_tokens: the ring is cut into nodes_per_dc equal
    slices, node k owning the k-th of them, as the "murmur3" partitioner of
    YCSB's Calvin micro-benchmark (CalvinPartitioner.initialToken) assumes.
    Tokens must be unique in the cluster, hence the offset by dc_index."""
    if k == nodes_per_dc:
        token = 2**63 - 1
    else:
        token = -2**63 + k * (2**64 // nodes_per_dc) - 1
    return token - dc_index


def fixed_tokens_enabled():
    return str(config.get("cassandra.fixed_tokens", 0)).lower() in ("1", "true")


def create_cassandra_cluster(num_dcs, nodes_per_dc, cassandra_image):
    network_name = config["network_name"]
    is_real = infra.is_real()
    extra_hosts = build_extra_hosts(nodes_per_dc) if is_real else None

    nano_cpus = None
    mem_limit = None
    vcpus = None
    cassandra_xms = "2g"
    cassandra_xmx = "4g"
    cassandra_direct = None
    machine = infra.machine_shape()
    ephemeral_read_enabled = config.get("accord.ephemeral_read", "true")
    if machine:
        vm_csv = os.path.join(os.path.dirname(__file__), '..', 'vm.csv')
        shape = None
        try:
            with open(vm_csv, 'r') as vm_file:
                for row in csv.DictReader(vm_file):
                    if row['name'] == machine:
                        shape = row
                        break
        except FileNotFoundError:
            debug(f"vm.csv not found: {vm_csv}")
            exit(-1)

        # infra/simulation/provider.sh:infra_resource_limits() treats an unknown
        # machine as a hard error, so this path has to as well.  Carrying on
        # with a guess is how the loop variable used to leak: a `machine` absent
        # from vm.csv left the last row bound, and its vcpus were handed to
        # -XX:ActiveProcessorCount while the heap fell back to its default.
        if shape is None:
            debug(f"Machine type '{machine}' not found in {vm_csv}")
            exit(-1)

        vcpus = shape['vcpus']
        nano_cpus = int(float(vcpus) * 1e9)
        memory_gb = float(shape['memory'])
        mem_limit = int(memory_gb * 1024 * 1024 * 1024 * 4/5)

        # The heap has to fit *inside* the container, with room to spare: the
        # cgroup also has to hold off-heap structures, direct buffers, thread
        # stacks, metaspace and page cache, and none of that is charged to the
        # heap.  Sizing it from memory_gb rather than from the container's own
        # limit put -Xmx8g inside a 6.4 GiB cgroup, so the JVM was entitled to
        # more than the kernel would give it: GC pauses reached 20.7s (nodetool
        # gcstats) and the containers were eventually SIGKILLed with exit 137.
        container_gb = memory_gb * 4/5
        xmx_gb = max(1, round(container_gb * 0.6))

        # -Xms == -Xmx.  conf/jvm-server.options carries -XX:+AlwaysPreTouch, so
        # the heap is committed and faulted in while the node starts -- which we
        # already wait out, see log_pattern below -- instead of growing from 2g
        # across the measurement window, which maxexecutiontime keeps short
        # enough that the growth would land inside it.
        cassandra_xms = f"{xmx_gb}g"
        cassandra_xmx = f"{xmx_gb}g"

        # cassandra-env.sh sizes MaxDirectMemorySize in calculate_heap_sizes()
        # from `free -m`, which is not cgroup-aware: it reads the *host's* RAM
        # and, on anything above ~62 GiB, saturates at its hardcoded 15872M cap
        # no matter what this container's limit is.  Heap plus that ceiling then
        # sits within a few GiB of mem_limit, leaving nothing for metaspace,
        # thread stacks, Netty and page cache.  Pin it to a share of the
        # container instead; the env var wins over the calculated value.
        cassandra_direct = f"{max(1, round(container_gb * 0.15))}g"

    # Cassandra's own channel for extra JVM flags: cassandra-env.sh appends
    # JVM_EXTRA_OPTS on its last line, so these win over the -Xms/-Xmx that
    # script derives for itself.  JVM_OPTS must NOT be set here: bin/cassandra.in.sh
    # appends the jvm*.options files to whatever JVM_OPTS already holds rather
    # than resetting it, and bin/nodetool runs `java $JVM_OPTS -Xmx128m`, so an
    # inherited -Xms larger than 128m makes every nodetool invocation die with
    # "Initial heap size set to a larger value than the maximum heap size".
    # profiler_jvm_opts() rides along in the same var for the same reason: it
    # must stay out of JVM_OPTS, or nodetool (used by cassandra/profiler.sh
    # itself, via dexec) inherits -Daccord.debug_execution etc. too.
    jvm_env = {
        "JVM_EXTRA_OPTS": " -Xms" + cassandra_xms + " -Xmx" + cassandra_xmx +
                          (" -XX:ActiveProcessorCount=" + vcpus if vcpus else "") +
                          profiler_jvm_opts() + extra_jvm_opts(),
    }
    if cassandra_direct:
        jvm_env["MAX_DIRECT_MEMORY_SIZE"] = cassandra_direct

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
                        **jvm_env,
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
                if fixed_tokens_enabled():
                    # The image's entrypoint sets num_tokens but not
                    # initial_token: append it to cassandra.yaml first (once,
                    # should the container be restarted).
                    run_kwargs['environment']['CASSANDRA_NUM_TOKENS'] = "1"
                    run_kwargs['environment']['CASSANDRA_INITIAL_TOKEN'] = str(initial_token(k, nodes_per_dc, i - 1))
                    run_kwargs['entrypoint'] = [
                        "sh", "-c",
                        'grep -q "^initial_token:" "$CASSANDRA_CONF/cassandra.yaml"'
                        ' || echo "initial_token: $CASSANDRA_INITIAL_TOKEN" >> "$CASSANDRA_CONF/cassandra.yaml";'
                        ' exec docker-entrypoint.sh "$@"',
                        "sh"]
                    run_kwargs['command'] = ["cassandra", "-f"]
                if nano_cpus is not None:
                    run_kwargs['nano_cpus'] = nano_cpus
                if mem_limit is not None:
                    run_kwargs['mem_limit'] = mem_limit
                if is_real:
                    # One machine per node already gives each container its
                    # own network namespace; join the host's instead of a
                    # bridge that only exists (if at all) on this one daemon.
                    run_kwargs['network_mode'] = 'host'
                    run_kwargs['extra_hosts'] = extra_hosts
                    # Left unset, the image's entrypoint defaults
                    # broadcast_address to listen_address, i.e. the address
                    # Cassandra auto-detects from the host interface -- the
                    # private IP. Peers gossip back to whatever a node
                    # broadcasts, so an unset broadcast_address is why a
                    # cross-region node never rejoins the ring even once the
                    # security group and --add-host aliases correctly route
                    # traffic *to* it: every peer is told to reply to a
                    # private address it cannot reach.
                    broadcast_ip, _ = infra.host_for(container_name)
                    if broadcast_ip:
                        run_kwargs['environment']['CASSANDRA_BROADCAST_ADDRESS'] = broadcast_ip
                        run_kwargs['environment']['CASSANDRA_BROADCAST_RPC_ADDRESS'] = broadcast_ip
                else:
                    run_kwargs['network'] = network_name
                container = infra.client_for(container_name).containers.run(**run_kwargs)
                containers.append(container)
                debug(f"Starting container '{container_name}' in DC '{dc_name}', rack 'RAC{k}'.")
                if not wait_for_log(container, log_pattern):
                    debug(f"Failed to start container '{container_name}' within timeout.")
                    exit(-1)
            except docker.errors.APIError as e:
                print(f"ERROR: failed to start container '{container_name}': {e}", file=sys.stderr)
                sys.exit(1)

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

        latencies_file = infra.locations_file()
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
