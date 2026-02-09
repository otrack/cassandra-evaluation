#!/usr/bin/env python3
"""
Plotting script for the closed economy experiment.

This script generates a grouped bar chart (histogram) showing:
- X-axis: number of nodes in the system (3, 4, 5)
- Y-axis: latency in milliseconds
- One bar group per node count, with bars for each protocol/percentile combination

The plot style is similar to Fig. 9 from https://arxiv.org/pdf/2104.01142
"""

import csv
import math
import os
import sys
import pandas as pd
import numpy as np

MAX_PERCENTILE = 100
UNKNOWN_VALUE = "unknown"
METRIC_COLUMNS = {"avg": "avg_latency_ms", "p90": "p90_ms", "p95": "p95_ms", "p99": "p99_ms"}
METRIC_LABELS = {"avg": "avg", "p90": "P90", "p95": "P95", "p99": "P99"}
LATENCY_METRICS = tuple(METRIC_COLUMNS.keys())
MIN_BAR_WIDTH = 0.12  # centimeters (explicit units used in pgfplots bar width)
BAR_WIDTH_TOTAL = 0.9  # centimeters allocated across all series in a group
DEFAULT_BAR_WIDTH = 0.2  # centimeters when no series exist


def usage_and_exit():
    print("Usage: python closed_economy.py results.csv output.tex")
    sys.exit(1)

def calculate_bar_width(series_count):
    """Return the bar width in centimeters for the given series count."""
    if series_count <= 0:
        return DEFAULT_BAR_WIDTH
    return max(MIN_BAR_WIDTH, BAR_WIDTH_TOTAL / series_count)

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

def percentile_value(row, percentile):
    """Return a percentile latency value (ms) from a DataFrame row."""
    key = f"p{percentile}"
    v = row.get(key, None)
    if pd.isna(v):
        return None
    if isinstance(v, str) and v.strip().lower() == UNKNOWN_VALUE:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None

def estimate_row_latency(row):
    """Estimate mean latency (ms) from percentile columns in a DataFrame row.

    Args:
        row: Pandas Series/dict containing percentile keys (p1..p100).

    Returns:
        Estimated latency in milliseconds, or None if no valid percentile data exists.
    """
    vals = []
    for i in range(1, MAX_PERCENTILE + 1):
        v = percentile_value(row, i)
        if v is not None:
            vals.append(v)
    if not vals:
        return None
    return np.mean(vals)

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
    df_rmw['avg_latency_ms'] = df_rmw.apply(estimate_row_latency, axis=1)
    for percentile in (90, 95, 99):
        df_rmw[f"p{percentile}_ms"] = df_rmw.apply(
            lambda row, p=percentile: percentile_value(row, p), axis=1
        )

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

    # Compute average latency per protocol and node count
    data = {}
    for proto in protocols:
        data[proto] = {}
        for nodes in node_counts:
            subset = df_rmw[(df_rmw['protocol'] == proto) & (df_rmw['nodes_int'] == nodes)]
            if not subset.empty:
                data[proto][nodes] = {}
                for metric in LATENCY_METRICS:
                    col = METRIC_COLUMNS[metric]
                    vals = subset[col].dropna()
                    data[proto][nodes][metric] = float(np.mean(vals)) if not vals.empty else 0
            else:
                data[proto][nodes] = {metric: 0 for metric in LATENCY_METRICS}

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
            for metric in LATENCY_METRICS:
                val = data[proto].get(nodes, {}).get(metric, 0)
                if val > 0:
                    all_vals.append(val)
    if all_vals:
        ymax = max(all_vals) * 1.2
    else:
        ymax = 100

    # Generate TikZ/pgfplots code for grouped bar chart
    series_count = len(protocols) * len(LATENCY_METRICS)
    bar_width = calculate_bar_width(series_count)

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=8cm,\n")
        f.write("      ybar,\n")
        f.write(f"      bar width={bar_width:.2f}cm,\n")
        f.write("      enlarge x limits=0.25,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      xlabel={Number of nodes},\n")
        f.write("      ylabel={Latency (ms)},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      symbolic x coords={" + ",".join(str(n) for n in node_counts) + "},\n")
        f.write("      xtick=data,\n")
        f.write("      legend pos=north east,\n")
        f.write("      legend style={font=\\small},\n")
        f.write("    ]\n\n")

        series_idx = 0
        for proto in protocols:
            for metric in LATENCY_METRICS:
                col = color_cycle[series_idx % len(color_cycle)]
                series_idx += 1
                f.write(f"      \\addplot+[fill={col}, draw=black] coordinates {{\n")
                for nodes in node_counts:
                    val = data[proto].get(nodes, {}).get(metric, 0)
                    f.write(f"        ({nodes}, {val:.2f})\n")
                f.write("      };\n")
                f.write(f"      \\addlegendentry{{{proto} - {METRIC_LABELS[metric]}}}\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Closed economy workload latency (read-modify-write transactions) "
                "as a function of the number of nodes. Each group shows results for a given number of nodes, "
                "with bars representing the average latency and tail percentiles per protocol.}\n")
        f.write("  \\label{fig:closed-economy-latency}\n")
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
