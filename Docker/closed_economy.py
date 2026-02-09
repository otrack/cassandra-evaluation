#!/usr/bin/env python3
"""
Plotting script for the closed economy experiment.

This script generates a grouped bar chart (histogram) showing:
- X-axis: number of nodes in the system (3, 4, 5)
- Y-axis: number of transactions (throughput in ops/sec)
- One bar group per node count, with bars for each protocol (accord, cockroachdb)

The plot style is similar to Fig. 9 from https://arxiv.org/pdf/2104.01142
"""

import csv
import math
import os
import sys
import pandas as pd
import numpy as np


def usage_and_exit():
    print("Usage: python closed_economy.py results.csv output.tex")
    sys.exit(1)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def estimate_latency(distance_km):
    fiber_optic_speed_km_per_ms = 204
    latency_ms = distance_km / fiber_optic_speed_km_per_ms
    return math.floor(latency_ms)

def load_locations(latencies_path):
    locations = []
    try:
        with open(latencies_path, newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                try:
                    lat = float(row['lat'])
                    lon = float(row['lon'])
                except (KeyError, TypeError, ValueError):
                    continue
                locations.append((lat, lon))
    except FileNotFoundError:
        return []
    return locations

def row_mean_latency(row):
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
        except (TypeError, ValueError):
            continue
    if not vals:
        return None
    return float(np.mean(vals))

def estimate_row_throughput(row):
    tput = None
    try:
        tput = float(row.get('tput', 0))
    except (TypeError, ValueError):
        tput = None
    if tput is not None and tput > 0:
        return tput
    mean_latency_ms = row_mean_latency(row)
    if mean_latency_ms is None or mean_latency_ms <= 0:
        return None
    try:
        clients = int(row.get('clients', 1))
    except (TypeError, ValueError):
        clients = 1
    if clients <= 0:
        clients = 1
    return (clients * 1000.0) / mean_latency_ms

def compute_e(n, f):
    e = 0
    for candidate in range(n + 1):
        if n >= max(2 * candidate + f - 1, 2 * f + 1):
            e = candidate
    return e

def round_trip_to_quorum(locations, quorum_size):
    n = len(locations)
    if n == 0:
        return []
    rtts = []
    for i in range(n):
        dists = []
        for j in range(n):
            if i == j:
                continue
            lat1, lon1 = locations[i]
            lat2, lon2 = locations[j]
            dists.append(haversine(lat1, lon1, lat2, lon2))
        if not dists:
            rtts.append(0.0)
            continue
        dists.sort()
        needed = max(1, quorum_size - 1)
        selected = dists[:min(needed, len(dists))]
        rtts.append(2 * estimate_latency(max(selected)))
    return rtts

def round_trip_to_nearest_nodes(locations, count):
    n = len(locations)
    if n == 0:
        return []
    rtts = []
    for i in range(n):
        dists = []
        for j in range(n):
            if i == j:
                continue
            lat1, lon1 = locations[i]
            lat2, lon2 = locations[j]
            dists.append(haversine(lat1, lon1, lat2, lon2))
        if not dists:
            rtts.append(0.0)
            continue
        dists.sort()
        selected = dists[:min(count, len(dists))]
        rtts.append(2 * estimate_latency(max(selected)))
    return rtts

def accord_latency_bounds(locations):
    n = len(locations)
    if n == 0:
        return None, None
    f = (n - 1) // 2
    e = compute_e(n, f)
    fast_quorum = max(1, n - e)
    slow_quorum = max(1, n - f)
    fast_rtts = round_trip_to_quorum(locations, fast_quorum)
    slow_rtts = round_trip_to_quorum(locations, slow_quorum)
    execute_rtts = round_trip_to_nearest_nodes(locations, 2)
    best_vals = [fast_rtts[i] + execute_rtts[i] for i in range(n)]
    worst_vals = [3 * slow_rtts[i] + execute_rtts[i] for i in range(n)]
    return float(np.mean(best_vals)), float(np.mean(worst_vals))

def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]

    df = pd.read_csv(results_csv)

    # Filter for tx-readmodifywrite operations (the transaction operation in closed economy)
    df_rmw = df[df['op'] == 'tx-readmodifywrite'].copy()

    # If no tx-readmodifywrite data, fall back to all data
    if df_rmw.empty:
        print(f"Invalid data")
        exit(1)
        
    # Parse nodes as integers
    def safe_int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    df_rmw['nodes_int'] = df_rmw['nodes'].apply(safe_int)
    df_rmw = df_rmw[df_rmw['nodes_int'].notnull()]
    df_rmw['tput_est'] = df_rmw.apply(estimate_row_throughput, axis=1)

    # Get unique protocols and node counts, sorted
    protocols = sorted(df_rmw['protocol'].unique().tolist())
    node_counts = sorted(df_rmw['nodes_int'].unique().tolist())

    latencies_path = os.path.join(os.path.dirname(__file__), "latencies.csv")
    locations = load_locations(latencies_path)
    accord_latencies = []
    if locations:
        for nodes in node_counts:
            if nodes <= len(locations):
                best_latency, worst_latency = accord_latency_bounds(locations[:nodes])
                if best_latency is not None and worst_latency is not None:
                    accord_latencies.append((nodes, best_latency, worst_latency))

    # Compute average throughput per protocol and node count
    # Throughput is in the 'tput' column (ops/sec)
    data = {}
    for proto in protocols:
        data[proto] = {}
        for nodes in node_counts:
            subset = df_rmw[(df_rmw['protocol'] == proto) & (df_rmw['nodes_int'] == nodes)]
            if not subset.empty:
                # Parse throughput values
                tput_vals = []
                for val in subset['tput_est']:
                    if val is None or pd.isna(val):
                        continue
                    try:
                        tput_vals.append(float(val))
                    except (TypeError, ValueError):
                        continue
                if tput_vals:
                    data[proto][nodes] = np.mean(tput_vals)
                else:
                    data[proto][nodes] = 0
            else:
                data[proto][nodes] = 0

    # Prepare colors for protocols
    color_cycle = [
        "blue!80!black",
        "red!80!black",
        "green!60!black",
        "orange!80!black",
        "purple!80!black",
        "cyan!80!black",
    ]

    # Compute y-axis limits
    all_vals = []
    for proto in protocols:
        for nodes in node_counts:
            if data[proto].get(nodes, 0) > 0:
                all_vals.append(data[proto][nodes])
    if all_vals:
        ymax = max(all_vals) * 1.2
    else:
        ymax = 100

    # Generate TikZ/pgfplots code for grouped bar chart
    bar_width = 0.8 / len(protocols)  # width per bar

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=8cm,\n")
        f.write("      ybar,\n")
        f.write("      bar width=0.4cm,\n")
        f.write("      enlarge x limits=0.25,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      xlabel={Number of nodes},\n")
        f.write("      ylabel={Throughput (transactions/sec)},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      symbolic x coords={" + ",".join(str(n) for n in node_counts) + "},\n")
        f.write("      xtick=data,\n")
        f.write("      legend pos=north east,\n")
        f.write("      legend style={font=\\small},\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocols):
            col = color_cycle[idx % len(color_cycle)]
            f.write(f"      \\addplot+[fill={col}, draw=black] coordinates {{\n")
            for nodes in node_counts:
                val = data[proto].get(nodes, 0)
                f.write(f"        ({nodes}, {val:.2f})\n")
            f.write("      };\n")
            f.write(f"      \\addlegendentry{{{proto}}}\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Throughput of the closed economy workload (read-modify-write transactions) "
                "as a function of the number of nodes. Each group shows results for a given number of nodes, "
                "with bars representing different protocols.}\n")
        f.write("  \\label{fig:closed-economy-throughput}\n")
        f.write("\\end{figure}\n")

        if accord_latencies:
            f.write("\\medskip\n")
            f.write("\\begin{tabular}{lrr}\n")
            f.write("  \\hline\n")
            f.write("  Nodes & Accord best-case latency (ms) & Accord worst-case latency (ms) \\\\\n")
            f.write("  \\hline\n")
            for nodes, best_latency, worst_latency in accord_latencies:
                f.write(f"  {nodes} & {best_latency:.2f} & {worst_latency:.2f} \\\\\n")
            f.write("  \\hline\n")
            f.write("\\end{tabular}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
