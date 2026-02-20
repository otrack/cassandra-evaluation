import sys
import math
import pandas as pd

from emulate_latency import haversine, estimate_latency

def compute_optimum_per_replica(latlon, n_nodes):
    """
    For each replica i (0..n_nodes-1), compute the RTT to the closest quorum
    (majority) of replicas.

    Algorithm:
      - quorum_size = floor(n_nodes/2) + 1 (majority)
      - for replica i, compute haversine distances to all other replicas
      - take the (quorum_size - 1) nearest other replicas (since the local replica
        itself counts toward the quorum)
      - the RTT for replica i is 2 * estimate_latency(max_distance) where
        max_distance is the distance to the farthest replica in the quorum
    Returns a list of RTTs (same order as latlon[0..n_nodes-1]).
    """
    optimums = []
    # quorum (majority)
    quorum = (n_nodes // 2) + 1

    for i in range(n_nodes):
        dists = []
        for j in range(n_nodes):
            if i == j:
                continue
            dist = haversine(latlon[i][0], latlon[i][1], latlon[j][0], latlon[j][1])
            dists.append(dist)

        if not dists:
            # single-node case -> no neighbour, fallback to 0
            optimums.append(0.0)
            continue

        # sort ascending and pick the nearest (quorum - 1) other replicas
        dists.sort()
        need = max(1, quorum - 1)  # at least 1 if quorum>1, safe if quorum==1
        selected = dists[:min(need, len(dists))]

        # RTT to reach a quorum is determined by the slowest member in that quorum
        max_dist = max(selected)
        rtt = 2*estimate_latency(max_dist)
        optimums.append(rtt)

    return optimums

def escape_latex(text):
    """Escape special LaTeX characters in a string."""
    replacements = [
        ('\\', '\\textbackslash{}'),
        ('&', '\\&'),
        ('%', '\\%'),
        ('$', '\\$'),
        ('#', '\\#'),
        ('_', '\\_'),
        ('{', '\\{'),
        ('}', '\\}'),
        ('~', '\\textasciitilde{}'),
        ('^', '\\textasciicircum{}'),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    return text

def get_global_latency_range(df, workloads, all_ops, num_nodes):
    min_latency = float('inf')
    max_latency = float('-inf')
    for workload in workloads:
        for op in all_ops:
            dfw = df[(df['workload'] == workload) & (df['nodes'] == num_nodes)
                     & (df['op'] == op)]
            for _, row in dfw.iterrows():
                latencies = [float(row[f'p{i}']) for i in range(1, 101)
                             if pd.notnull(row.get(f'p{i}')) and row[f'p{i}'] != 'unknown']
                if latencies:
                    min_latency = min(min_latency, min(latencies))
                    max_latency = max(max_latency, max(latencies))
    if min_latency == float('inf'): min_latency = 0
    if max_latency == float('-inf'): max_latency = 1
    return min_latency, max_latency

def compute_average_latencies_across_cities(df, workload, op, num_nodes):
    """
    Compute average latencies across all cities in the dataframe for a specific workload and operation.

    For each protocol, averages the p1-p100 values across all cities.
    Returns a dict: {protocol: [avg_p1, avg_p2, ..., avg_p100]}
    """
    averages = {}

    # Get data for all cities for this workload and operation
    dfw = df[(df['workload'] == workload) &
             (df['nodes'] == num_nodes) &
             (df['op'] == op)]

    if dfw.empty:
        return averages

    # Group by protocol
    for proto in dfw['protocol'].unique():
        proto_rows = dfw[dfw['protocol'] == proto]

        # Collect and average latency percentiles across all cities
        latency_sums = {}
        latency_counts = {}

        for _, row in proto_rows.iterrows():
            for i in range(1, 101):
                p_col = f'p{i}'
                if pd.notnull(row.get(p_col)) and row[p_col] != 'unknown':
                    val = float(row[p_col])
                    if i not in latency_sums:
                        latency_sums[i] = 0.0
                        latency_counts[i] = 0
                    latency_sums[i] += val
                    latency_counts[i] += 1

        # Compute averages for each percentile
        avg_latencies = []
        for i in range(1, 101):
            if i in latency_counts and latency_counts[i] > 0:
                avg_latencies.append(latency_sums[i] / latency_counts[i])
            else:
                avg_latencies.append(None)

        if avg_latencies:
            averages[proto] = avg_latencies

    return averages

def main():
    if len(sys.argv) < 7:
        print(
            "Usage: python cdf.py results.csv workload1 [workload2 ...] num_nodes city1 [city2 ...] latitudes.csv output.tex [--average]"
        )
        sys.exit(1)

    results_csv = sys.argv[1]

    # Check for --average flag (must be last)
    include_average = False
    if sys.argv[-1] == "--average":
        include_average = True
        output_tikz = sys.argv[-2]
        lat_csv = sys.argv[-3]
        args_end_idx = -3
    else:
        output_tikz = sys.argv[-1]
        lat_csv = sys.argv[-2]
        args_end_idx = -2

    # Parse workloads, num_nodes, cities
    remaining = sys.argv[2:args_end_idx]

    # Find num_nodes (first integer)
    num_nodes = None
    num_nodes_idx = None
    for idx, arg in enumerate(remaining):
        try:
            num_nodes = int(arg)
            num_nodes_idx = idx
            break
        except ValueError:
            pass

    if num_nodes is None:
        print("ERROR: Could not find num_nodes (an integer) in arguments", file=sys.stderr)
        sys.exit(1)

    workloads = remaining[:num_nodes_idx]
    cities = remaining[num_nodes_idx + 1:]

    if not workloads or not cities:
        print("ERROR: Must specify at least one workload and one city", file=sys.stderr)
        sys.exit(1)

    # Load data
    df_unfiltered = pd.read_csv(results_csv)
    df = df_unfiltered.copy()
    no_cities = False

    # Filter for plotting by listed cities
    if 'city' in df.columns:
        df_filtered = df[df['city'].isin(cities)]
        if df_filtered.empty:
            available_cities = df['city'].unique().tolist()
            print(f"WARNING: No data found for cities {cities}. Available cities: {available_cities}", file=sys.stderr)
            no_cities = True
            if include_average:
                print(f"Plotting only the average", file=sys.stderr)
            else:
                print(f"Nothing to plot. Leaving", file=sys.stderr)
                sys.exit(1)
        else:
            df = df_filtered

    latdf = pd.read_csv(lat_csv)
    node_lats, node_lons, node_locs = [], [], []
    for idx, row in latdf.iloc[:num_nodes].iterrows():
        try:
            node_lats.append(float(row['lat']))
            node_lons.append(float(row['lon']))
            node_locs.append(row['loc'] if 'loc' in row else '')
        except Exception as e:
            print(f"WARNING: Skipping row {idx+2} in latitudes.csv due to error: {e}", file=sys.stderr)
    latlon = list(zip(node_lats, node_lons))

    # Compute theoretical optimum for each listed city
    city_optimums = {}
    for city in cities:
        city_index = None
        for idx, loc in enumerate(node_locs):
            if loc == city:
                city_index = idx
                break
        if city_index is not None and len(latlon) > 0:
            optimums = compute_optimum_per_replica(latlon, num_nodes)
            if city_index < len(optimums):
                city_optimums[city] = optimums[city_index]

    # Compute theoretical optimum for ALL cities in latitudes file
    all_city_optimums = {}
    for idx, loc in enumerate(node_locs):
        if len(latlon) > 0:
            optimums = compute_optimum_per_replica(latlon, num_nodes)
            if idx < len(optimums):
                all_city_optimums[loc] = optimums[idx]

    # Compute average optimum across ALL cities
    avg_optimum = None
    if all_city_optimums:
        avg_optimum = sum(all_city_optimums.values()) / len(all_city_optimums)

    # Get all unique operations
    all_ops = set()
    for workload in workloads:
        subdf = df[(df['workload'] == workload) & (df['nodes'] == num_nodes) ]
        all_ops.update(subdf['op'].unique())
    all_ops = sorted(list(all_ops))
    n_ops = len(all_ops)
    n_wl = len(workloads)
    n_cities = len(cities)

    actual_cities = df['city'].unique().tolist() if 'city' in df.columns and not no_cities else []
    actual_n_cities = len(actual_cities)

    # Calculate total rows
    if include_average:
        total_rows = n_wl + (actual_n_cities * n_wl)
    else:
        total_rows = actual_n_cities * n_wl

    # Protocol order for consistent color assignment
    if no_cities:
        df_for_protocols = df_unfiltered
    else:
        df_for_protocols = df

    protocol_order = []
    for workload in workloads:
        for op in all_ops:
            dfw = df_for_protocols[(df_for_protocols['workload'] == workload) & (df_for_protocols['nodes'] == num_nodes)
                                   & (df_for_protocols['op'] == op)]
            for proto in dfw['protocol'].unique():
                if proto not in protocol_order:
                    protocol_order.append(proto)

    min_latency, max_latency = get_global_latency_range(df, workloads, all_ops, num_nodes)
    xpad = 0.05 * (max_latency - min_latency)
    min_latency = max(0, min_latency - xpad)
    max_latency = max_latency + xpad

    color_cycle = [
        "red", "blue", "green!50!black", "cyan!80!black",
        "magenta!80!black", "yellow!80!black", "black"
    ]

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("    \\centering\n")
        f.write("    \\begin{tikzpicture}\n")

        f.write("      \\begin{groupplot}[\n")
        f.write(f"        group style={{group size={n_ops} by {total_rows}, horizontal sep=1.2cm, vertical sep=1.2cm}},\n")
        f.write("        width=4cm, height=4cm,\n")
        f.write("        grid=both,\n")
        f.write("        ymajorgrids=true,\n")
        f.write("        xmajorgrids=true,\n")
        f.write("        ymin=0, ymax=1,\n")
        f.write(f"        xmin={0:.2f},\n")
        f.write(f"        xmax={500:.2f},\n")
        f.write("        ytick={0,0.5,1},\n")
        f.write("        cycle list name=color list,\n")
        f.write("      ]\n")

        # First, plot average rows if requested
        if include_average:
            for wl_index, workload in enumerate(workloads):
                for op_index, op in enumerate(all_ops):
                    # Compute average latencies from unfiltered data (all cities)
                    avg_latencies_dict = compute_average_latencies_across_cities(df_unfiltered, workload, op, num_nodes)

                    f.write("        \\nextgroupplot[\n")
                    if op_index == 0:
                        if no_cities:
                            f.write(f"          ylabel={{{workload}}},\n")
                        else:
                            f.write(f"          ylabel={{average ({workload})}},\n")
                    else:
                        f.write("          yticklabels={{}},\n")
                    if wl_index == n_wl - 1:
                        f.write("          xlabel={{Latency (ms)}},\n")
                    if wl_index == 0:
                        f.write(f"          title={{{op}}},\n")
                    f.write("        ]\n")

                    if not avg_latencies_dict:
                        continue

                    for proto_idx, proto in enumerate(protocol_order):
                        if proto not in avg_latencies_dict:
                            continue

                        latencies = avg_latencies_dict[proto]
                        if not latencies:
                            continue

                        col = color_cycle[proto_idx % len(color_cycle)]
                        f.write("          \\addplot+["+col+", mark=none] table {\n")
                        for i, val in enumerate(latencies):
                            pct = i/99
                            f.write(f"          {val} {pct}\n")
                        f.write("          };\n")

                    # Draw vertical gray line for the average optimum (across all cities)
                    if avg_optimum is not None:
                        f.write(f"          \\draw[gray, thick] (axis cs:{avg_optimum:.2f},0) -- (axis cs:{avg_optimum:.2f},1);\n")

        # Then, plot city rows
        if not no_cities:
            for city_index, city in enumerate(cities):
                for wl_index, workload in enumerate(workloads):
                    for op_index, op in enumerate(all_ops):
                        dfw = df[(df['workload'] == workload) & (df['nodes'] == num_nodes)
                                 & (df['op'] == op) & (df['city'] == city)]

                        if dfw.empty:
                            continue

                        f.write("        \\nextgroupplot[\n")
                        if op_index == 0:
                            if actual_n_cities == 1 and not include_average:
                                f.write(f"          ylabel={{{workload}}},\n")
                            else:
                                f.write(f"          ylabel={{{city} ({workload})}},\n")
                        else:
                            f.write("          yticklabels={{}},\n")
                        if city_index == n_cities - 1 and wl_index == n_wl - 1:
                            f.write("          xlabel={{Latency (ms)}},\n")
                        if wl_index == 0 and city_index == 0 and not include_average:
                            f.write(f"          title={{{op}}},\n")
                        f.write("        ]\n")

                        for proto_idx, proto in enumerate(protocol_order):
                            if proto not in dfw['protocol'].unique():
                                continue
                            row = dfw[dfw['protocol'] == proto].iloc[0] if not dfw[dfw['protocol'] == proto].empty else None
                            if row is None:
                                continue
                            latencies = [row[f'p{i}'] for i in range(1, 101)
                                         if pd.notnull(row.get(f'p{i}')) and row[f'p{i}'] != 'unknown']
                            if not latencies:
                                continue
                            col = color_cycle[proto_idx % len(color_cycle)]
                            f.write("          \\addplot+["+col+", mark=none] table {\n")
                            for i, val in enumerate(latencies):
                                pct = i/99
                                f.write(f"          {val} {pct}\n")
                            f.write("          };\n")

                        # Draw vertical gray line for the theoretical optimum (for listed city)
                        if city in city_optimums:
                            f.write(f"          \\draw[gray, thick] (axis cs:{city_optimums[city]:.2f},0) -- (axis cs:{city_optimums[city]:.2f},1);\n")

        f.write("      \\end{groupplot}\n")
        f.write("    \\end{tikzpicture}\n")

        # --- Caption with color swatches ---
        cities_escaped = ", ".join([escape_latex(c) for c in actual_cities])
        if include_average and no_cities:
            caption_start = f"CDF of average operation latencies for different YCSB workloads and replication protocols."
        elif include_average:
            caption_start = f"CDF of operation latencies for different YCSB workloads and replication protocols at {cities_escaped} (and average)."
        else:
            caption_start = f"CDF of operation latencies for different YCSB workloads and replication protocols at {cities_escaped}."

        f.write(f"    \\caption{{{caption_start} ")
        for proto_idx, proto in enumerate(protocol_order):
            col = color_cycle[proto_idx % len(color_cycle)]
            f.write(r"\protect\tikz \protect\draw[thick, {color}] (0,0) -- +(0.8,0);~{{{proto}}}".format(color=col, proto=proto))
            if proto_idx < len(protocol_order) - 1:
                f.write(", ")
        # Add reference to the gray optimum line in the caption
        if city_optimums or avg_optimum:
            f.write(". ")
            f.write(r"\protect\tikz \protect\draw[thick, gray] (0,0) -- +(0.8,0);~{optimum}")
        f.write("    \\label{fig:workload-cdf}}\n")
        f.write("\\end{figure}\n")

if __name__ == "__main__":
    main()
