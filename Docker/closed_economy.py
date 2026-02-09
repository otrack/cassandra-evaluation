#!/usr/bin/env python3
"""
Plotting script for the closed economy experiment.

This script generates a grouped error bar chart showing:
- X-axis: number of nodes in the system (3, 4, 5)
- Y-axis: latency in milliseconds
- One error bar group per node count and protocol, with dots for avg, tail percentiles, best, and worst

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
METRIC_COLUMNS = {
    "avg": "avg_latency_ms",
    "p90": "p90_ms",
    "p95": "p95_ms",
    "p99": "p99_ms",
    "best": "best_latency_ms",
    "worst": "worst_latency_ms",
}
METRIC_LABELS = {
    "avg": "Avg",
    "p90": "P90",
    "p95": "P95",
    "p99": "P99",
    "best": "Best",
    "worst": "Worst",
}
LATENCY_METRICS = ("avg", "p90", "p95", "p99", "best", "worst")
MARKER_METRICS = ("p90", "p95", "p99", "best", "worst")
METRIC_MARKS = {
    "p90": "triangle*",
    "p95": "square*",
    "p99": "diamond*",
    "best": "o",
    "worst": "x",
}
MIN_BAR_WIDTH = 0.12  # minimum readable bar width scalar (used with cm in output)
BAR_WIDTH_TOTAL = 0.9  # total bar width scalar allocated across all series in a group
DEFAULT_BAR_WIDTH = 0.2  # fallback bar width scalar when no series exist


def usage_and_exit():
    print("Usage: python closed_economy.py results.csv output.tex")
    sys.exit(1)

def calculate_bar_width(series_count):
    """Return the bar width scalar for the given series count.

    Args:
        series_count: Number of bar series in each node group.

    Returns:
        Bar width scalar used with cm units in the pgfplots output; defaults to
        DEFAULT_BAR_WIDTH when series_count <= 0.
    """
    if series_count <= 0:
        return DEFAULT_BAR_WIDTH
    # Distribute BAR_WIDTH_TOTAL across series while keeping a minimum width for readability.
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
    """Return a percentile latency value (ms) from a DataFrame row.

    Args:
        row: Pandas Series/dict containing percentile keys (p1..p100).
        percentile: Percentile to extract (e.g., 90 for p90).

    Returns:
        Float percentile latency in milliseconds, or None when missing/invalid.
    """
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

def percentile_values(row):
    values = []
    for i in range(1, MAX_PERCENTILE + 1):
        v = percentile_value(row, i)
        if v is not None:
            values.append(v)
    return values

def estimate_row_latency(row):
    """Estimate mean latency (ms) from percentile columns in a DataFrame row.

    Args:
        row: Pandas Series/dict containing percentile keys (p1..p100).

    Returns:
        Estimated latency in milliseconds, or None if no valid percentile data exists.
    """
    vals = percentile_values(row)
    if not vals:
        return None
    return np.mean(vals)

def row_best_latency(row):
    values = percentile_values(row)
    return min(values) if values else None

def row_worst_latency(row):
    values = percentile_values(row)
    return max(values) if values else None

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
    df_rmw['best_latency_ms'] = df_rmw.apply(row_best_latency, axis=1)
    df_rmw['worst_latency_ms'] = df_rmw.apply(row_worst_latency, axis=1)
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

    # Generate TikZ/pgfplots code for grouped error bar chart
    series_count = len(protocols)
    bar_width = calculate_bar_width(series_count)

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=8cm,\n")
        f.write(f"      bar width={bar_width:.2f}cm,\n")
        f.write("      enlarge x limits=0.25,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      xlabel={Number of nodes},\n")
        f.write("      ylabel={Latency (ms)},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      xtick={" + ",".join(str(i) for i in range(len(node_counts))) + "},\n")
        f.write("      xticklabels={" + ",".join(str(n) for n in node_counts) + "},\n")
        f.write("      legend pos=north east,\n")
        f.write("      legend style={font=\\small},\n")
        f.write("    ]\n\n")

        offset_step = 0.2
        offsets = [
            (idx - (len(protocols) - 1) / 2) * offset_step for idx in range(len(protocols))
        ]
        for proto_idx, proto in enumerate(protocols):
            col = color_cycle[proto_idx % len(color_cycle)]
            offset = offsets[proto_idx]
            f.write("      \\addplot+[\n")
            f.write(f"        color={col}, mark=*,\n")
            f.write("        error bars/.cd,\n")
            f.write("        y dir=both, y explicit,\n")
            f.write("      ] coordinates {\n")
            for pos, nodes in enumerate(node_counts):
                avg_val = data[proto].get(nodes, {}).get("avg", 0)
                best_val = data[proto].get(nodes, {}).get("best", 0)
                worst_val = data[proto].get(nodes, {}).get("worst", 0)
                if avg_val <= 0:
                    continue
                err_plus = max(0.0, worst_val - avg_val)
                err_minus = max(0.0, avg_val - best_val)
                x = pos + offset
                f.write(
                    f"        ({x:.2f}, {avg_val:.2f}) += (0, {err_plus:.2f}) -= (0, {err_minus:.2f})\n"
                )
            f.write("      };\n")
            f.write(f"      \\addlegendentry{{{proto}}}\n\n")
            for metric in MARKER_METRICS:
                mark = METRIC_MARKS[metric]
                f.write(f"      \\addplot+[only marks, mark={mark}, color={col}, forget plot] coordinates {{\n")
                for pos, nodes in enumerate(node_counts):
                    val = data[proto].get(nodes, {}).get(metric, 0)
                    if val <= 0:
                        continue
                    x = pos + offset
                    f.write(f"        ({x:.2f}, {val:.2f})\n")
                f.write("      };\n\n")

        for metric in MARKER_METRICS:
            mark = METRIC_MARKS[metric]
            f.write(f"      \\addlegendimage{{only marks, mark={mark}}}\n")
            f.write(f"      \\addlegendentry{{{METRIC_LABELS[metric]}}}\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Closed economy workload latency (read-modify-write transactions) "
                "as a function of the number of nodes. Each group shows results for a given number of nodes, "
                "with error bars showing best/worst latency and markers for the average and tail percentiles per protocol.}\n")
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
