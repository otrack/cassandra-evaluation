#!/usr/bin/env python3
"""
Plotting script for the latency vs throughput experiment.

This script generates a classical latency vs throughput graph showing:
- X-axis: Throughput (operations/sec)
- Y-axis: Latency (milliseconds)
- One line per protocol showing how latency increases as load increases
- Demonstrates the "hockey stick" effect where both latency and throughput degrade
"""

import sys
import pandas as pd
import numpy as np


def usage_and_exit():
    print("Usage: python latency_throughput.py results.csv output.tex")
    sys.exit(1)


def row_mean_latency(row):
    """Compute mean latency from p1..p100 percentiles.
    
    Note: This is an approximation. We compute the mean of percentile values
    to get a rough estimate of average latency. This approach is consistent
    with other plotting scripts in this codebase (e.g., conflict.py).
    """
    vals = []
    for i in range(1, 101):
        key = f"p{i}"
        v = row.get(key, None)
        if pd.isna(v):
            continue
        if isinstance(v, str) and v.strip().lower() == "unknown":
            continue
        try:
            vals.append(float(v))
        except Exception:
            continue
    if not vals:
        return None
    return float(np.mean(vals))


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]

    df = pd.read_csv(results_csv)

    # Parse numeric values
    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    def safe_int(x):
        try:
            return int(x)
        except Exception:
            return None

    df['tput_f'] = df['tput'].apply(safe_float)
    df['clients_int'] = df['clients'].apply(safe_int)

    # Filter to valid rows
    df = df[df['tput_f'].notnull() & df['clients_int'].notnull()].copy()

    if df.empty:
        print("No valid data found in results CSV.")
        sys.exit(1)

    # Compute mean latency for each row
    mean_lats = []
    for idx, row in df.iterrows():
        m = row_mean_latency(row)
        mean_lats.append(m)
    df['mean_latency_ms'] = mean_lats

    # Drop rows without computed mean latency
    df = df[df['mean_latency_ms'].notnull()]

    if df.empty:
        print("No rows with valid latency data.")
        sys.exit(1)

    # Get unique protocols in order of appearance
    protocol_order = []
    for proto in df['protocol'].unique():
        if proto not in protocol_order:
            protocol_order.append(proto)

    # For each protocol, aggregate by number of clients
    # Group by protocol and clients, compute average throughput and latency
    data_by_protocol = {}
    for proto in protocol_order:
        dfp = df[df['protocol'] == proto]
        
        # Group by number of clients
        client_counts = sorted(dfp['clients_int'].unique().tolist())
        
        throughputs = []
        latencies = []
        
        for clients in client_counts:
            df_clients = dfp[dfp['clients_int'] == clients]
            if not df_clients.empty:
                avg_tput = df_clients['tput_f'].mean()
                avg_lat = df_clients['mean_latency_ms'].mean()
                throughputs.append(avg_tput)
                latencies.append(avg_lat)
            else:
                throughputs.append(None)
                latencies.append(None)
        
        data_by_protocol[proto] = {
            'clients': client_counts,
            'throughputs': throughputs,
            'latencies': latencies
        }

    # Prepare colors (consistent with other scripts)
    color_cycle = [
        "red", "blue", "green!50!black", "cyan!80!black",
        "magenta!80!black", "yellow!80!black", "black"
    ]

    # Determine axis ranges
    all_tputs = []
    all_lats = []
    for proto, data in data_by_protocol.items():
        for t, l in zip(data['throughputs'], data['latencies']):
            if t is not None:
                all_tputs.append(t)
            if l is not None:
                all_lats.append(l)

    if all_tputs and all_lats:
        xmin = 0
        xmax = max(all_tputs) * 1.1
        ymin = 0
        ymax = max(all_lats) * 1.2
    else:
        xmin = 0
        xmax = 1000
        ymin = 0
        ymax = 100

    # Generate TikZ/pgfplots code
    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=8cm,\n")
        f.write("      grid=both,\n")
        f.write("      xlabel={Throughput (ops/sec)},\n")
        f.write("      ylabel={Average Latency (ms)},\n")
        f.write(f"      xmin={xmin:.2f}, xmax={xmax:.2f},\n")
        f.write(f"      ymin={ymin:.2f}, ymax={ymax:.2f},\n")
        f.write("      legend pos=north west,\n")
        f.write("      cycle list name=color list,\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocol_order):
            col = color_cycle[idx % len(color_cycle)]
            data = data_by_protocol[proto]
            
            f.write(f"      \\addplot+[{col}, mark=*, thick] table {{\n")
            for tput, lat in zip(data['throughputs'], data['latencies']):
                if tput is not None and lat is not None:
                    f.write(f"        {tput:.2f} {lat:.2f}\n")
            f.write("      };\n")
            f.write(f"      \\addlegendentry{{{proto}}}\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Latency vs Throughput: Average operation latency as a function of throughput. ")
        f.write("Each curve represents a protocol; the number of clients increases by a factor of 2 ")
        f.write("(1, 2, 4, 8, ...) until both latency and throughput degrade (hockey stick effect).}\n")
        f.write("  \\label{fig:latency-throughput}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
