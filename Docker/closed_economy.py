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

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, make_protocol_legend

MAX_PERCENTILE = 100
UNKNOWN_VALUE = "unknown"
METRIC_COLUMNS = {
    "avg": "median_latency_ms",
    "p90": "p90_ms",
    "p95": "p95_ms",
    "p99": "p99_ms",
    "best": "best_latency_ms",
    "worst": "worst_latency_ms",
}
METRIC_LABELS = {
    "avg": "Median",
    "p90": "P90",
    "p95": "P95",
    "p99": "P99",
    "best": "Best",
    "worst": "Worst",
}
LATENCY_METRICS = ("avg", "p90", "p95", "p99", "best", "worst")
MARKER_METRICS = ("avg", "p90", "p95", "p99")
METRIC_MARKS = {
    "avg": "*",
    "p90": "triangle*",
    "p95": "square*",
    "p99": "diamond*",
}
# Best latency is shown as the lower end of the vertical range, so no marker entry.
# pgfplots marker styles for percentile and extrema metrics (P90/P95/P99/worst).
MIN_OFFSET_STEP = 0.12  # minimum spacing between protocol groups
OFFSET_TOTAL = 0.9  # total spacing allocated across all protocol groups
DEFAULT_OFFSET_STEP = 0.2  # fallback spacing when no protocols exist


def usage_and_exit():
    print("Usage: python closed_economy.py results.csv breakdown.csv output.tex")
    sys.exit(1)

def calculate_offset_step(series_count):
    """Return the x-offset step for the given protocol count.

    Args:
        series_count: Number of protocol series in each node group.

    Returns:
        Offset step scalar for spacing protocol series; defaults to DEFAULT_OFFSET_STEP
        when series_count <= 0.
    """
    if series_count <= 0:
        return DEFAULT_OFFSET_STEP
    # Distribute OFFSET_TOTAL across protocols while keeping a minimum spacing.
    return max(MIN_OFFSET_STEP, OFFSET_TOTAL / series_count)

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
    """Collect percentile latency values (ms) from a DataFrame row.

    Args:
        row: Pandas Series/dict containing percentile keys (p1..p100).

    Returns:
        List of non-None percentile latency values in milliseconds.
    """
    values = []
    for i in range(1, MAX_PERCENTILE + 1):
        v = percentile_value(row, i)
        if v is not None:
            values.append(v)
    return values

def estimate_row_latency(row):
    """Return the median latency (p50) from a DataFrame row.

    Args:
        row: Pandas Series/dict containing percentile keys (p1..p100).

    Returns:
        Median latency in milliseconds (p50), or None if missing/invalid.
    """
    return percentile_value(row, 50)

def get_row_best_worst_latency(row):
    """Estimate best and worst latency (ms) from percentile columns in a DataFrame row.

    Args:
        row: Pandas Series/dict containing percentile keys (p1..p100).

    Returns:
        Tuple of (best_latency_ms, worst_latency_ms), or (None, None) if missing.
    """
    values = percentile_values(row)
    if not values:
        return None, None
    return min(values), max(values)

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

def load_breakdown(breakdown_csv):
    """Load breakdown data from breakdown.csv.

    Returns a dict: {protocol: {nodes: {city: {fast_commit, slow_commit, ordering, execution}}}}
    Values are in microseconds.
    """
    result = {}
    try:
        df = pd.read_csv(breakdown_csv)
    except (FileNotFoundError, IOError):
        return result

    required = {'protocol', 'nodes', 'city', 'fast_commit', 'slow_commit', 'ordering', 'execution'}
    if not required.issubset(df.columns):
        return result

    for _, row in df.iterrows():
        proto = str(row['protocol']).strip()
        try:
            nodes = int(row['nodes'])
        except (TypeError, ValueError):
            continue
        city = str(row['city']).strip()
        try:
            fc = float(row['fast_commit'])
            sc = float(row['slow_commit'])
            od = float(row['ordering'])
            ex = float(row['execution'])
        except (TypeError, ValueError):
            continue
        result.setdefault(proto, {}).setdefault(nodes, {})[city] = {
            'fast_commit': fc,
            'slow_commit': sc,
            'ordering': od,
            'execution': ex,
        }
    return result


def compute_average_breakdown(breakdown_data, protocol, nodes):
    """Return the average breakdown across all cities for (protocol, nodes).

    Returns a dict {fast_commit, slow_commit, ordering, execution} in
    microseconds, or None if no data are available.
    """
    cities_data = breakdown_data.get(protocol, {}).get(nodes, {})
    if not cities_data:
        return None
    components = ['fast_commit', 'slow_commit', 'ordering', 'execution']
    totals = {c: 0.0 for c in components}
    n = len(cities_data)
    for city_bd in cities_data.values():
        for c in components:
            totals[c] += city_bd.get(c, 0.0)
    return {c: totals[c] / n for c in components}


# nodes value used for the breakdown subplot (first/smallest experiment)
DEFAULT_FIRST_NODES = 3
BREAKDOWN_COMPONENTS = ['fast_commit', 'slow_commit', 'ordering', 'execution']
BREAKDOWN_LABELS = ['Fast commit', 'Slow commit', 'Ordering', 'Execution']
BREAKDOWN_PATTERNS = ['north east lines', 'north west lines', 'horizontal lines', 'vertical lines']
BREAKDOWN_COLORS_LATEX = ['blue!40', 'red!60', 'green!50!black', 'orange!80']


def main():
    if len(sys.argv) < 4:
        usage_and_exit()

    results_csv = sys.argv[1]
    breakdown_csv = sys.argv[2]
    output_tikz = sys.argv[3]

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
    df_rmw['median_latency_ms'] = df_rmw.apply(estimate_row_latency, axis=1)
    df_rmw[['best_latency_ms', 'worst_latency_ms']] = df_rmw.apply(
        get_row_best_worst_latency, axis=1, result_type='expand'
    )
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
                    data[proto][nodes][metric] = float(np.mean(vals)) if not vals.empty else None
            else:
                data[proto][nodes] = {metric: None for metric in LATENCY_METRICS}

    # Prepare colors from the unified protocol color schema
    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    # Compute y-axis limits
    all_vals = []
    for proto in protocols:
        for nodes in node_counts:
            for metric in LATENCY_METRICS:
                val = data[proto].get(nodes, {}).get(metric)
                if val is not None:
                    all_vals.append(val)
    if all_vals:
        ymax = max(all_vals) * 1.2
    else:
        ymax = 100

    # Load breakdown data for the second plot
    breakdown_data = load_breakdown(breakdown_csv)

    # Compute breakdown averages for nodes=3 (first experiment)
    first_nodes = min(node_counts) if node_counts else DEFAULT_FIRST_NODES
    bd_protocols = sorted(breakdown_data.keys())
    breakdown_avgs = {}
    for proto in bd_protocols:
        avg = compute_average_breakdown(breakdown_data, proto, first_nodes)
        if avg is not None:
            breakdown_avgs[proto] = avg

    # Compute y-axis limit for breakdown chart (microseconds)
    bd_all_vals = []
    for proto, avg in breakdown_avgs.items():
        total = sum(avg.values())
        bd_all_vals.append(total)
    bd_ymax = max(bd_all_vals) * 1.2 if bd_all_vals else 1000

    # Generate TikZ/pgfplots code for grouped latency range chart
    # Group by protocol on the x-axis with offsets per node count.
    series_count = len(node_counts)
    offset_step = calculate_offset_step(series_count)

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocols, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\begin{tikzpicture}[scale=.7]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=8cm, height=8cm,\n")
        f.write("      enlarge x limits=0.25,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      xlabel={Protocol},\n")
        f.write("      ylabel={Latency (ms)},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      xtick={" + ",".join(str(i) for i in range(len(protocols))) + "},\n")
        f.write("      xticklabels={" + ",".join(protocols) + "},\n")
        f.write("    ]\n\n")

        # Center node count offsets around each protocol position.
        offsets = [
            (idx - (len(node_counts) - 1) / 2) * offset_step for idx in range(len(node_counts))
        ]
        for proto_idx, proto in enumerate(protocols):
            col = get_protocol_color(proto, protocol_colors, proto_idx)
            for node_idx, nodes in enumerate(node_counts):
                offset = offsets[node_idx]
                avg_val = data[proto].get(nodes, {}).get("avg")
                best_val = data[proto].get(nodes, {}).get("best")
                worst_val = data[proto].get(nodes, {}).get("worst")
                if avg_val is None or best_val is None or worst_val is None:
                    continue
                # Only compare bounds once we know all values are present.
                # Skip inconsistent data where bounds do not enclose the average.
                if not (best_val <= avg_val <= worst_val):
                    continue
                x = proto_idx + offset
                # Two coordinates at the same x-position draw a vertical line for best/worst.
                f.write(f"      \\addplot+[mark=-, color={col}, solid, forget plot] coordinates {{\n")
                f.write(f"        ({x:.2f}, {best_val:.2f})\n")
                f.write(f"        ({x:.2f}, {worst_val:.2f})\n")
                f.write("      };\n\n")
                f.write(f"      \\addplot+[only marks, mark=*, color={col}, mark options=fill={col}, forget plot] coordinates {{\n")
                f.write(f"        ({x:.2f}, {avg_val:.2f})\n")
                f.write("      };\n\n")
                for metric in MARKER_METRICS:
                    mark = METRIC_MARKS[metric]
                    val = data[proto].get(nodes, {}).get(metric)
                    if val is None:
                        continue
                    f.write(f"      \\addplot+[only marks, mark={mark}, color={col}, mark options=fill={col}, forget plot] coordinates {{\n")
                    f.write(f"        ({x:.2f}, {val:.2f})\n")
                    f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")

        # Second plot: stacked bar breakdown for nodes=3
        if breakdown_avgs:
            bd_proto_list = sorted(breakdown_avgs.keys())
            bd_proto_aliases = [protocol_aliases.get(p, p) for p in bd_proto_list]

            f.write("  \\hspace{1cm}\n")
            f.write("  \\begin{tikzpicture}[scale=.7]\n")

            # Breakdown legend
            bd_legend_entries = []
            for comp, label, color in zip(BREAKDOWN_COMPONENTS, BREAKDOWN_LABELS, BREAKDOWN_COLORS_LATEX):
                bd_legend_entries.append(
                    r"\protect\tikz \protect\fill[{color}] (0,0) rectangle (0.3,0.3);"
                    r"~{label}".format(color=color, label=label)
                )
            f.write("    {{\\small {}}}\\\\[4pt]\n".format(r"\quad ".join(bd_legend_entries)))

            f.write("    \\begin{axis}[\n")
            f.write("      ybar stacked,\n")
            f.write("      width=8cm, height=8cm,\n")
            f.write("      bar width=0.5cm,\n")
            f.write("      enlarge x limits=0.5,\n")
            f.write("      ymajorgrids=true,\n")
            f.write("      xlabel={Protocol},\n")
            f.write("      ylabel={Average latency ($\\mu$s)},\n")
            f.write(f"      ymin=0, ymax={bd_ymax:.2f},\n")
            f.write("      xtick={" + ",".join(str(i) for i in range(len(bd_proto_list))) + "},\n")
            f.write("      xticklabels={" + ",".join(bd_proto_aliases) + "},\n")
            f.write("    ]\n\n")

            for comp, label, color, pattern in zip(
                BREAKDOWN_COMPONENTS, BREAKDOWN_LABELS, BREAKDOWN_COLORS_LATEX, BREAKDOWN_PATTERNS
            ):
                coords = []
                for i, proto in enumerate(bd_proto_list):
                    val = breakdown_avgs[proto].get(comp, 0.0)
                    coords.append(f"({i}, {val:.2f})")
                f.write(f"      \\addplot+[ybar, fill={color}, draw=black,"
                        f" pattern={pattern}, pattern color={color}] coordinates {{\n")
                f.write("        " + " ".join(coords) + "\n")
                f.write("      };\n\n")

            f.write("    \\end{axis}\n")
            f.write("  \\end{tikzpicture}\n")

        f.write("  \\caption{\\label{fig:closed-economy-latency}\n"
                " Left: Closed economy workload latency as a function of the protocol."
                " For each protocol, from left to right, 3, 5 and 7 nodes."
                " The markers indicate the median ($\\CIRCLE$), P90 ($\\blacktriangle$),"
                " P95 ($\\blacksquare$), and P99 ($\\blacklozenge$) percentiles."
                " Right: Average latency breakdown per phase for nodes=3,"
                " averaged across all data centers.}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
