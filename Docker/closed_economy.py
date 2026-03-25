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

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, make_protocol_legend, sort_protocols_for_legend, sort_protocols_for_plotting

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
    print("Usage: python closed_economy.py results.csv breakdown.csv output.tex [threads_default]")
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

    Returns a dict: {protocol: {nodes: {city: {fast_commit, slow_commit, commit, ordering, execution}}}}
    Values are in microseconds.
    """
    result = {}
    try:
        df = pd.read_csv(breakdown_csv)
    except (FileNotFoundError, IOError):
        return result

    required = {'protocol', 'nodes', 'city', 'fast_commit', 'slow_commit', 'commit', 'ordering', 'execution'}
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
            cm = float(row['commit'])
            od = float(row['ordering'])
            ex = float(row['execution'])
        except (TypeError, ValueError):
            continue
        result.setdefault(proto, {}).setdefault(nodes, {})[city] = {
            'fast_commit': fc,
            'slow_commit': sc,
            'commit': cm,
            'ordering': od,
            'execution': ex,
        }
    return result


def compute_average_breakdown(breakdown_data, protocol, nodes):
    """Return the average breakdown across all cities for (protocol, nodes).

    Returns a dict {fast_commit, slow_commit, commit, ordering, execution} in
    microseconds, or None if no data are available.
    """
    cities_data = breakdown_data.get(protocol, {}).get(nodes, {})
    if not cities_data:
        return None
    components = ['fast_commit', 'slow_commit', 'commit', 'ordering', 'execution']
    totals = {c: 0.0 for c in components}
    n = len(cities_data)
    for city_bd in cities_data.values():
        for c in components:
            totals[c] += city_bd.get(c, 0.0)
    return {c: totals[c] / n for c in components}


# nodes value used for the breakdown subplot (first/smallest experiment)
# fast_commit and slow_commit are loaded from the CSV but consolidated into commit for plotting.
# Plotting the average commit avoids showing two separate bars (fast/slow path) that may
# be misleading; the weighted average commit is the single representative commit latency.
BREAKDOWN_COMPONENTS = ['commit', 'ordering', 'execution']
BREAKDOWN_LABELS = ['Commit', 'Ordering', 'Execution']
BREAKDOWN_PATTERNS = ['north east lines', 'horizontal lines', 'vertical lines']
BREAKDOWN_COLORS_LATEX = ['blue!40', 'green!50!black', 'orange!80']
# Node counts shown in the breakdown subplot (one stacked bar per protocol×nodes combination)
BREAKDOWN_ALL_NODE_COUNTS = [3, 5, 7]
# Breakdown CSV values are stored in microseconds; convert to milliseconds for plotting.
MICROS_TO_MILLIS = 1000.0


def main():
    if len(sys.argv) < 4:
        usage_and_exit()

    results_csv = sys.argv[1]
    breakdown_csv = sys.argv[2]
    output_tikz = sys.argv[3]
    # Optional: thread count used for the multi-client runs (for axis label "client/DC=N")
    try:
        multi_client_threads = int(sys.argv[4]) if len(sys.argv) >= 5 else None
    except ValueError:
        multi_client_threads = None

    df = pd.read_csv(results_csv)

    # Filter for tx-readmodifywrite operations (the transaction operation in closed economy)
    df_rmw = df[df['op'] == 'tx-readmodifywrite'].copy()

    # If no tx-readmodifywrite data, fall back to all data
    if df_rmw.empty:
        print(f"Invalid data")
        exit(1)
        
    # Parse nodes and clients as integers
    def safe_int(x):
        try:
            return int(x)
        except (TypeError, ValueError):
            return None

    df_rmw['nodes_int'] = df_rmw['nodes'].apply(safe_int)
    df_rmw['clients_int'] = df_rmw['clients'].apply(safe_int)
    df_rmw = df_rmw[df_rmw['nodes_int'].notnull()]
    df_rmw['median_latency_ms'] = df_rmw.apply(estimate_row_latency, axis=1)
    df_rmw[['best_latency_ms', 'worst_latency_ms']] = df_rmw.apply(
        get_row_best_worst_latency, axis=1, result_type='expand'
    )
    for percentile in (90, 95, 99):
        df_rmw[f"p{percentile}_ms"] = df_rmw.apply(
            lambda row, p=percentile: percentile_value(row, p), axis=1
        )

    # Split into single-client (1 thread per DC) and multi-client runs
    df_single = df_rmw[df_rmw['clients_int'] == 1].copy()
    if df_single.empty:
        # Fallback: treat all data as single-client
        df_single = df_rmw.copy()

    if multi_client_threads is not None:
        df_multi = df_rmw[df_rmw['clients_int'] == multi_client_threads].copy()
    else:
        # Auto-detect: take rows with clients > 1 (largest value wins)
        non_one = df_rmw[df_rmw['clients_int'] != 1]['clients_int'].dropna()
        if not non_one.empty:
            multi_client_threads = int(non_one.max())
            df_multi = df_rmw[df_rmw['clients_int'] == multi_client_threads].copy()
        else:
            df_multi = pd.DataFrame()

    has_multi = not df_multi.empty

    # Get unique protocols and node counts; sort protocols by protocols.csv order.
    protocols_legend = sort_protocols_for_legend(df_single['protocol'].unique().tolist())
    protocols_plot   = sort_protocols_for_plotting(df_single['protocol'].unique().tolist())
    node_counts = sorted(df_single['nodes_int'].unique().tolist())

    latencies_path = os.path.join(os.path.dirname(__file__), "latencies.csv")
    locations = load_locations(latencies_path)
    accord_latencies = []
    if locations:
        for nodes in node_counts:
            if nodes <= len(locations):
                best_latency, worst_latency = accord_latency_bounds(locations[:nodes])
                if best_latency is not None and worst_latency is not None:
                    accord_latencies.append((nodes, best_latency, worst_latency))

    # Compute average latency per protocol and node count (single-client data)
    data = {}
    for proto in protocols_plot:
        data[proto] = {}
        for nodes in node_counts:
            subset = df_single[(df_single['protocol'] == proto) & (df_single['nodes_int'] == nodes)]
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

    # Compute y-axis limits for the left (single-client) plot
    all_vals = []
    for proto in protocols_plot:
        for nodes in node_counts:
            for metric in LATENCY_METRICS:
                val = data[proto].get(nodes, {}).get(metric)
                if val is not None:
                    all_vals.append(val)
    if all_vals:
        ymax = max(all_vals) * 1.2
    else:
        ymax = 100

    # Load breakdown data for the breakdown subplot (used only when no multi-client data)
    breakdown_data = load_breakdown(breakdown_csv)

    # Compute breakdown averages for all node counts (3, 5, 7), one entry per
    # (protocol, nodes) pair that has data.  The averages are taken across all
    # cities (data centres) for that combination.
    # Sort breakdown protocols in protocols.csv order with Accord last.
    bd_protocols = sort_protocols_for_plotting(list(breakdown_data.keys()))
    breakdown_items = []          # ordered list of (proto, nodes) with data
    breakdown_avgs_all = {}       # {(proto, nodes): avg_dict}
    for proto in bd_protocols:
        for n in BREAKDOWN_ALL_NODE_COUNTS:
            avg = compute_average_breakdown(breakdown_data, proto, n)
            if avg is not None:
                breakdown_avgs_all[(proto, n)] = avg
                breakdown_items.append((proto, n))
                
    # Compute multi-client data for the comparison right subplot
    all_protocols_right = protocols_plot  # default: same as left
    data_multi = {}
    if has_multi:
        protocols_multi_plot = sort_protocols_for_plotting(df_multi['protocol'].unique().tolist())
        # Combined protocol order for the right subplot (both sections share the same x positions)
        all_protocols_right = sort_protocols_for_plotting(
            list(dict.fromkeys(protocols_plot + protocols_multi_plot))
        )
        for proto in all_protocols_right:
            data_multi[proto] = {}
            for nodes in node_counts:
                subset = df_multi[(df_multi['protocol'] == proto) & (df_multi['nodes_int'] == nodes)]
                if not subset.empty:
                    data_multi[proto][nodes] = {}
                    for metric in LATENCY_METRICS:
                        col = METRIC_COLUMNS[metric]
                        vals = subset[col].dropna()
                        data_multi[proto][nodes][metric] = float(np.mean(vals)) if not vals.empty else None
                else:
                    data_multi[proto][nodes] = {metric: None for metric in LATENCY_METRICS}

    # Generate TikZ/pgfplots code for grouped latency range chart
    # Group by protocol on the x-axis with offsets per node count.
    series_count = len(node_counts)
    offset_step = calculate_offset_step(series_count)

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        # Protocol legend in protocols.csv order
        f.write(make_protocol_legend(protocols_legend, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        # Breakdown legend placed before both tikzpictures so both plots remain on the same line.
        if breakdown_items:
            bd_legend_entries = []
            for comp, label, color in zip(BREAKDOWN_COMPONENTS, BREAKDOWN_LABELS, BREAKDOWN_COLORS_LATEX):
                bd_legend_entries.append(
                    r"\protect\tikz \protect\fill[{color}] (0,0) rectangle (0.3,0.3);"
                    r"~{label}".format(color=color, label=label)
                )
            f.write("  {{\\tiny {}}}\\\\[4pt]\n".format(r"\quad ".join(bd_legend_entries)))

        # ---- Left subplot: comparison of single-client vs multi-client latency ----
        # Two groups separated by a vertical dashed line.
        # When no multi-client data, falls back to single-client only (no separator).
        n = len(all_protocols_right)
        if has_multi:
            section_gap = 1.0  # gap between the two sections (for the dashed separator)
            section2_offset = n + section_gap
            dashed_x = n - 0.5 + section_gap / 2.0   # midpoint of the gap between sections
            label1_x = (n - 1) / 2.0                 # centre of section 1
            label2_x = section2_offset + (n - 1) / 2.0  # centre of section 2

            # Compute y-axis range from both sections
            left_vals = []
            for proto in all_protocols_right:
                for nodes in node_counts:
                    for src in (data, data_multi):
                        for metric in LATENCY_METRICS:
                            v = src.get(proto, {}).get(nodes, {}).get(metric)
                            if v is not None:
                                left_vals.append(v)
            left_ymax = max(left_vals) * 1.2 if left_vals else ymax

            xtick_positions = list(range(n)) + [section2_offset + i for i in range(n)]
            xticklabels = [protocol_aliases.get(p, p) for p in all_protocols_right] * 2
        else:
            # No multi-client data: show single-client group only
            left_ymax = ymax
            xtick_positions = list(range(n))
            xticklabels = [protocol_aliases.get(p, p) for p in all_protocols_right]

        # Center node count offsets around each protocol position.
        offsets = [
            (idx - (len(node_counts) - 1) / 2) * offset_step for idx in range(len(node_counts))
        ]

        f.write("  \\begin{tikzpicture}[scale=.6]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=6.5cm, height=6cm,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      ymode=log,\n")
        f.write("      ylabel={Latency (ms)},\n")
        f.write(f"     ymin=0, ymax={left_ymax:.2f},\n")
        f.write("      xticklabel=\\empty\n")
        f.write("    ]\n\n")

        # Part 1: single-client data
        for proto_idx, proto in enumerate(all_protocols_right):
            col = get_protocol_color(proto, protocol_colors, proto_idx)
            for node_idx, nodes in enumerate(node_counts):
                offset = offsets[node_idx]
                avg_val = data.get(proto, {}).get(nodes, {}).get("avg")
                best_val = data.get(proto, {}).get(nodes, {}).get("best")
                worst_val = data.get(proto, {}).get(nodes, {}).get("worst")
                if avg_val is None:
                    continue
                x = proto_idx + offset
                if best_val is not None and worst_val is not None and best_val <= avg_val <= worst_val:
                    f.write(f"      \\addplot+[mark=-, color={col}, solid, forget plot] coordinates {{\n")
                    f.write(f"        ({x:.2f}, {best_val:.2f})\n")
                    f.write(f"        ({x:.2f}, {worst_val:.2f})\n")
                    f.write("      };\n\n")
                f.write(f"      \\addplot+[only marks, mark=*, color={col}, mark options=fill={col}, forget plot] coordinates {{\n")
                f.write(f"        ({x:.2f}, {avg_val:.2f})\n")
                f.write("      };\n\n")
                for metric in MARKER_METRICS:
                    mark = METRIC_MARKS[metric]
                    val = data.get(proto, {}).get(nodes, {}).get(metric)
                    if val is None:
                        continue
                    f.write(f"      \\addplot+[only marks, mark={mark}, color={col}, mark options=fill={col}, forget plot] coordinates {{\n")
                    f.write(f"        ({x:.2f}, {val:.2f})\n")
                    f.write("      };\n\n")

        if has_multi:
            # Part 2: multi-client data
            for proto_idx, proto in enumerate(all_protocols_right):
                col = get_protocol_color(proto, protocol_colors, proto_idx)
                for node_idx, nodes in enumerate(node_counts):
                    offset = offsets[node_idx]
                    avg_val = data_multi.get(proto, {}).get(nodes, {}).get("avg")
                    best_val = data_multi.get(proto, {}).get(nodes, {}).get("best")
                    worst_val = data_multi.get(proto, {}).get(nodes, {}).get("worst")
                    if avg_val is None:
                        continue
                    x = section2_offset + proto_idx + offset
                    if best_val is not None and worst_val is not None and best_val <= avg_val <= worst_val:
                        f.write(f"      \\addplot+[mark=-, color={col}, solid, forget plot] coordinates {{\n")
                        f.write(f"        ({x:.2f}, {best_val:.2f})\n")
                        f.write(f"        ({x:.2f}, {worst_val:.2f})\n")
                        f.write("      };\n\n")
                    f.write(f"      \\addplot+[only marks, mark=*, color={col}, mark options=fill={col}, forget plot] coordinates {{\n")
                    f.write(f"        ({x:.2f}, {avg_val:.2f})\n")
                    f.write("      };\n\n")
                    for metric in MARKER_METRICS:
                        mark = METRIC_MARKS[metric]
                        val = data_multi.get(proto, {}).get(nodes, {}).get(metric)
                        if val is None:
                            continue
                        f.write(f"      \\addplot+[only marks, mark={mark}, color={col}, mark options=fill={col}, forget plot] coordinates {{\n")
                        f.write(f"          ({x:.2f}, {val:.2f})\n")
                        f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        f.write(f"    \\node[font=\\tiny] at (1.2,-.5) {{1 client/DC}};\n")
        f.write(f"    \\node[font=\\tiny] at (4,-.5) {{{multi_client_threads} clients/DC}};\n")
        
        f.write("  \\end{tikzpicture}\n")

        # ---- Right subplot: stacked bar breakdown (single-client data) ----
        if breakdown_items:
            item_labels = [
                f"{protocol_aliases.get(p, p)}/{n}" for (p, n) in breakdown_items
            ]

            f.write("  \\begin{tikzpicture}[scale=.6]\n")
            f.write("    \\begin{axis}[\n")
            f.write("      ybar stacked,\n")
            f.write("      width=6cm, height=6cm,\n")
            f.write("      enlarge x limits=0.15,\n")
            f.write("      bar width=0.2cm,\n")
            f.write("      ymajorgrids=true,\n")
            f.write("      ymode=log,\n")
            f.write("      ylabel=\\empty,\n")
            f.write(f"     ymin=0, ymax={left_ymax:.2f},\n")
            f.write("      xticklabel=\\empty,\n")
            f.write("      yticklabel=\\empty\n")
            f.write("    ]\n\n")

            # Emit one \addplot per (bar, phase) so each bar can be filled with
            # its protocol color.  pgfplots tracks the stack height per x
            # position independently, so stacking works correctly even when the
            # \addplot commands for the same x are not consecutive.
            for i, (proto, n) in enumerate(breakdown_items):
                proto_color = get_protocol_color(proto, protocol_colors, i)
                for comp, label, bd_color, pattern in zip(
                    BREAKDOWN_COMPONENTS, BREAKDOWN_LABELS, BREAKDOWN_COLORS_LATEX, BREAKDOWN_PATTERNS
                ):
                    # Convert from microseconds (stored in CSV) to milliseconds.
                    val = breakdown_avgs_all.get((proto, n), {}).get(comp, 0.0) / MICROS_TO_MILLIS
                    if val == 0:
                        val = 1  # log scale requires a positive value
                    f.write(f"      \\addplot+[ybar, fill={proto_color}, draw=black,"
                            f" pattern={pattern}, pattern color={bd_color}] coordinates {{\n")
                    f.write(f"        ({i}, {val:.3f})\n")
                    f.write("      };\n\n")

            f.write("    \\end{axis}\n")
            f.write(f"    \\node[font=\\tiny] at (2.5,-.5) {{breakdown (1 client/DC)}};\n")
            f.write("  \\end{tikzpicture}\n")

        if has_multi:
            left_caption = (
                " Left: Closed economy workload latency for client/DC=1 (left of dashed line)"
                f" and client/DC={multi_client_threads} (right of dashed line)."
                " For each protocol, from left to right, 3, 5 and 7 nodes."
                " The markers indicate the median ($\\CIRCLE$), P90 ($\\blacktriangle$),"
                " P95 ($\\blacksquare$), and P99 ($\\blacklozenge$) percentiles."
                " CockroachDB* pins the lease holder at the geographically optimal location."
            )
        else:
            left_caption = (
                " Left: Closed economy workload latency as a function of the protocol."
                " For each protocol, from left to right, 3, 5 and 7 nodes."
                " The markers indicate the median ($\\CIRCLE$), P90 ($\\blacktriangle$),"
                " P95 ($\\blacksquare$), and P99 ($\\blacklozenge$) percentiles."
                " CockroachDB* pins the lease holder at the geographically optimal location."
            )

        if breakdown_items:
            right_caption = (
                " Right: Average latency breakdown per phase (ms) for 3, 5, and 7 nodes,"
                " averaged across all data centers."
            )
        else:
            right_caption = ""

        f.write("  \\caption{\\label{fig:closed-economy-latency}\n"
                f"{left_caption}{right_caption}}}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
