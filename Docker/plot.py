import re
import matplotlib.pyplot as plt
import sys
import glob
from theoritical_latency_approximation import *
from matplotlib.ticker import MaxNLocator

def extract_metric(file_path, metric_type, benchmark_mode):
    with open(file_path, 'r') as file:
        for line in file:
            if metric_type == "throughput":
                match = re.search(r'\[OVERALL\], Throughput\(ops/sec\), (\d+)', line)
            else:
                match = re.search(rf'\[{benchmark_mode.upper()}\], AverageLatency\(us\), ([\d.]+)', line)
            if match:
                return float(match.group(1))
    return None

def plot_metric(num_nodes_list, metric, protocol_name, output_file, metric_type):
    plt.figure(figsize=(10, 6))
    plt.plot(num_nodes_list, metric, marker='o', linestyle='-', color='b')
    plt.xlabel('Number of Nodes')
    plt.ylabel('Throughput (ops/sec)' if metric_type == "throughput" else 'Average Latency (us)')
    plt.title(f'YCSB Benchmark {metric_type.capitalize()} for {protocol_name} Protocol')
    plt.grid(True)
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))  # Ensure x-axis has integer ticks only
    plt.savefig(output_file)
    print(f"Graph saved as {output_file}")

def plot_combined_metric(num_nodes_list, metric, protocol_name):
    plt.plot(num_nodes_list, metric, marker='o', linestyle='-', label=protocol_name)
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))  # Ensure x-axis has integer ticks only

def save_combined_fig(benchmark_mode, metric_type):
    plt.legend()
    plt.xlabel('Number of Nodes')
    plt.ylabel(f'{metric_type.capitalize()} (ops/sec)' if metric_type == "throughput" else 'Average Latency (us)')
    plt.title(f'YCSB Benchmark {metric_type.capitalize()} for {benchmark_mode}')
    plt.grid(True)
    plt.gca().xaxis.set_major_locator(MaxNLocator(integer=True))  # Ensure x-axis has integer ticks only
    output_file = f"ycsb_{metric_type}_{benchmark_mode}_c.png"
    plt.savefig(output_file)
    print(f"Graph saved as {output_file}")

def process_files(pattern, metric_type, benchmark_mode):
    num_nodes_metric_list = []
    files = glob.glob(pattern)
    for file_path in files:
        match = re.search(r'serial_(\d+)_nodes_', file_path)
        if not match:
            match = re.search(r'quorum_(\d+)_nodes_', file_path)
        if match:
            num_nodes = int(match.group(1))
            metric = extract_metric(file_path, metric_type, benchmark_mode)
            if metric is not None:
                num_nodes_metric_list.append((num_nodes, metric))
            else:
                print(f"{metric_type.capitalize()} not found in file: {file_path}")
    num_nodes_metric_list.sort()
    num_nodes_list = [entry[0] for entry in num_nodes_metric_list]
    metric = [entry[1] for entry in num_nodes_metric_list]
    return num_nodes_list, metric

if __name__ == "__main__":
    if len(sys.argv) not in [2, 3]:
        print("Usage: python3 plot.py <protocol_name> <benchmark_mode> or python3 plot.py <benchmark_mode>")
        sys.exit(1)

    benchmark_modes = ["run", "load", "INSERT", "READ", "UPDATE"]

    if len(sys.argv) == 2:
        benchmark_mode = sys.argv[1]
        if benchmark_mode not in benchmark_modes:
            print("Usage: python3 plot.py <benchmark_mode>")
            print("<benchmark_mode> is one of 'run', 'load', 'INSERT', 'READ', 'UPDATE'")
            sys.exit(1)

        protocol_names = ["accord", "normal", "quorum"]
        metric_type = "throughput" if benchmark_mode in ["run", "load"] else "latency"
        plt.figure(figsize=(10, 6))
        max_num_nodes = 0
        max_num_nodes_list = []
        for protocol_name in protocol_names:
            if benchmark_mode == "INSERT":
                pattern = f"quorum_*_nodes_accord_load_c.txt" if protocol_name == "quorum" else f"serial_*_nodes_{protocol_name}_load_c.txt"
            else:
                pattern = f"quorum_*_nodes_accord_run_c.txt" if protocol_name == "quorum" else f"serial_*_nodes_{protocol_name}_run_c.txt"
            num_nodes_list, metric = process_files(pattern, metric_type, benchmark_mode)
            if len(num_nodes_list) > max_num_nodes:
                max_num_nodes = len(num_nodes_list)
                max_num_nodes_list = num_nodes_list.copy()
            if metric:
                plot_protocol_name = "paxos" if protocol_name == "normal" else protocol_name
                plot_combined_metric(num_nodes_list, metric, plot_protocol_name)
            else:
                print(f"No {metric_type} found to plot.")
        paxos_theoritical_approximations = [latency * 1000 for latency in [paxos_operaion_estimation(node_count) for node_count in max_num_nodes_list]]
        quorum_theoritical_approximations = [latency * 1000 for latency in [quorum_estimation(node_count) for node_count in max_num_nodes_list]]
        accord_fast_path_quotrient = 1.
        accord_theoritical_approximations = [latency * 1000 for latency in [accord_estimation(node_count, accord_fast_path_quotrient) for node_count in max_num_nodes_list]]
        plot_combined_metric(max_num_nodes_list, paxos_theoritical_approximations, "poxos theory")
        plot_combined_metric(max_num_nodes_list, quorum_theoritical_approximations, "quorum theory")
        plot_combined_metric(max_num_nodes_list, accord_theoritical_approximations, f"accord fast path")
        save_combined_fig(benchmark_mode, metric_type)
    else:
        protocol_name = sys.argv[1]
        benchmark_mode = sys.argv[2]
        metric_type = "throughput" if benchmark_mode in ["run", "load"] else "latency"
        if benchmark_mode == "INSERT":
            pattern = f"serial_*_nodes_{protocol_name}_load_c.txt"
        else:
            pattern = f"serial_*_nodes_{protocol_name}_run_c.txt"
        num_nodes_list, metric = process_files(pattern, metric_type, benchmark_mode)
        if metric:
            output_file = f"ycsb_{metric_type}_{protocol_name}_{benchmark_mode}.png"
            plot_metric(num_nodes_list, metric, protocol_name, output_file, metric_type)
        else:
            print(f"No {metric_type} found to plot.")
