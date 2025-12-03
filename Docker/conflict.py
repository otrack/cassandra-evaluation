#!/usr/bin/env python3
import sys
import csv
import math
import pandas as pd
import numpy as np

from emulate_latency import haversine, estimate_latency

def usage_and_exit():
    print("Usage: python conflict.py results.csv workload1 [workload2 ...] num_nodes latencies.csv output.tex")
    sys.exit(1)

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
        except Exception:
            continue
    if not vals:
        return None
    return float(np.mean(vals))

def compute_optimum_per_replica(latlon, n_nodes):
    """
    For each replica i (0..n_nodes-1), compute the RTT to the closest quorum
    (majority) of replicas.

    Algorithm:
      - quorum_size = floor(n_nodes/2) + 1 (majority)
      - for replica i, compute haversine distances to all other replicas
      - take the (quorum_size - 1) nearest other replicas (since the local replica
        itself counts toward the quorum)
      - the RTT for replica i is the largest of those selected distances divided
        by 100 (same units/scale used elsewhere)
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

def main():
    if len(sys.argv) < 6:
        usage_and_exit()

    results_csv = sys.argv[1]
    workloads = sys.argv[2:-3]
    try:
        num_nodes = int(sys.argv[-3])
    except Exception:
        print("num_nodes must be an integer")
        usage_and_exit()
    lat_csv = sys.argv[-2]   # kept for compatibility with the invocation signature
    output_tikz = sys.argv[-1]

    df = pd.read_csv(results_csv)

    # Ensure columns exist
    if 'conflict_rate' not in df.columns:
        print("ERROR: results CSV must contain 'conflict_rate' column (as produced by parse_ycsb_to_csv.sh).")
        sys.exit(1)

    # Normalize types
    # nodes column in parse script is a string, convert to int where possible
    if 'nodes' in df.columns:
        def safe_int(x):
            try:
                return int(x)
            except Exception:
                return None
        df['nodes_int'] = df['nodes'].apply(safe_int)
    else:
        df['nodes_int'] = None

    # Filter by workloads and nodes
    if workloads:
        df = df[df['workload'].isin(workloads)]
    if df['nodes_int'].notnull().any():
        df = df[df['nodes_int'] == num_nodes]

    # Convert conflict_rate to float where possible
    def parse_conflict(x):
        try:
            return float(x)
        except Exception:
            return np.nan
    df['conflict_rate_f'] = df['conflict_rate'].apply(parse_conflict)

    # Keep only rows with a parsable conflict rate
    df_valid = df[df['conflict_rate_f'].notnull()].copy()
    if df_valid.empty:
        print("No valid rows with a numeric conflict_rate found. Exiting.")
        sys.exit(0)

    # Compute per-row mean latency (ms) from p1..p100 percentiles
    mean_lats = []
    for idx, row in df_valid.iterrows():
        m = row_mean_latency(row)
        mean_lats.append(m)
    df_valid['mean_latency_ms'] = mean_lats

    # Drop rows without computed mean
    df_valid = df_valid[df_valid['mean_latency_ms'].notnull()]

    # Determine protocol order (stable)
    protocol_order = []
    for proto in df_valid['protocol'].unique():
        if proto not in protocol_order:
            protocol_order.append(proto)

    # x-axis: conflict rates from 0.0 to 1.0 step 0.1
    x_values = [round(x, 2) for x in np.arange(0.0, 1.0001, 0.1)]

    # For each protocol, compute average mean_latency_ms per conflict rate
    data_by_protocol = {}
    for proto in protocol_order:
        dfp = df_valid[df_valid['protocol'] == proto]
        rates = []
        for x in x_values:
            # Due to floating formatting in CSV, match with a tolerance
            df_rate = dfp[np.isclose(dfp['conflict_rate_f'].astype(float), x, atol=1e-6)]
            if df_rate.empty:
                # try matching formatted strings (e.g., "0.0" vs "0")
                df_rate = dfp[dfp['conflict_rate_f'].round(2) == round(x,2)]
            if df_rate.empty:
                rates.append(None)
            else:
                rates.append(float(df_rate['mean_latency_ms'].mean()))
        data_by_protocol[proto] = rates

    # Prepare colors (re-use same palette as cdf.py)
    color_cycle = [
        "red", "blue", "green!50!black", "cyan!80!black",
        "magenta!80!black", "yellow!80!black", "black"
    ]

    # Determine y range for nicer plotting
    all_vals = []
    for proto, vals in data_by_protocol.items():
        for v in vals:
            if v is not None:
                all_vals.append(v)
    if all_vals:
        ymin = max(0, min(all_vals) * 0.9)
        ymax = max(all_vals) * 1.1
    else:
        ymin = 0
        ymax = 1

    # --- Minimal addition: compute theoretical optimums from replica locations if available,
    # otherwise fall back to numeric latencies file parsing (backcompat).
    replica_means = []
    replica_labels = []
    try:
        # Read all rows, keep non-empty
        with open(lat_csv, newline='') as lf:
            reader = csv.reader(lf)
            rows = [r for r in reader if any(cell.strip() != '' for cell in r)]

        # If the first num_nodes rows look like lat,lon,loc (lat/lon numeric), use location-based optimums
        use_locations = False
        if len(rows) >= num_nodes:
            candidate = rows[1:1+num_nodes]
            ok = True
            for r in candidate:
                if len(r) < 2:
                    ok = False
                    break
                try:
                    float(r[0].strip())
                    float(r[1].strip())
                except Exception:
                    ok = False
                    break
            if ok:
                use_locations = True
                
        if use_locations:
            latlon = []
            for r in rows[1:1+num_nodes]:
                lat = float(r[0].strip())
                lon = float(r[1].strip())
                loc = r[2].strip() if len(r) >= 3 else ""
                latlon.append((lat, lon, loc))
            # compute per-replica optimum RTTs (same logic as cdf.compute_optimum_cdf)
            replica_means = compute_optimum_per_replica(latlon, num_nodes)
            # labels are the location names if present, otherwise generic
            replica_labels = [t[2] if t[2] else f"replica-{i}" for i, t in enumerate(latlon)]
        else:
            # fallback: try to parse remaining rows as a numeric clients x replicas matrix
            # (this preserves the script's previous behavior)
            numeric_rows = rows
            if len(rows) >= num_nodes:
                # treat first num_nodes rows as site locations (if they aren't lat/lon)
                numeric_rows = rows[num_nodes:]
                # attempt to extract labels from the first num_nodes rows (if present)
                loc_rows = rows[:num_nodes]
                replica_labels = [row[0].strip() if row and row[0].strip() != '' else f"replica-{i}" for i, row in enumerate(loc_rows)]
            mat = []
            for r in numeric_rows:
                vals = []
                for token in r:
                    token = token.strip()
                    if token == '':
                        continue
                    for st in token.replace(';', ' ').split():
                        if st == '':
                            continue
                        try:
                            vals.append(float(st))
                        except Exception:
                            # ignore non-numeric tokens
                            pass
                if vals:
                    mat.append(vals)
            if mat:
                cols = [len(r) for r in mat]
                if len(set(cols)) == 1:
                    arr = np.array(mat, dtype=float)
                    if arr.shape[1] >= num_nodes:
                        arr = arr[:, :num_nodes]
                    # fall back to per-replica mean from latency matrix
                    replica_means = list(np.nanmean(arr, axis=0))
                    if replica_labels and len(replica_labels) < len(replica_means):
                        replica_labels += [f"replica-{i}" for i in range(len(replica_labels), len(replica_means))]
            # if nothing parsed, replica_means remains empty and no lines will be drawn
    except Exception:
        # Do not fail the whole script if latencies parsing fails; just skip optimums
        replica_means = []
        replica_labels = []

    # Prepare an escaped label for the third entry (data replica) to include in the caption
    data_replica_caption = ""
    try:
        if len(replica_labels) >= 3:
            # take the third entry (index 2)
            raw_label = replica_labels[2]
            # minimal LaTeX escaping for common special chars (backslash, underscore, percent)
            safe = raw_label.replace("\\", "\\textbackslash{}").replace("_", "\\_").replace("%", "\\%")
            data_replica_caption = f" Data replica location: \\texttt{{{safe}}}."
    except Exception:
        data_replica_caption = ""

    # Write TikZ/pgfplots file
    with open(output_tikz, "w") as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=6cm,\n")
        f.write("      grid=both,\n")
        f.write("      xlabel={Conflict rate ($\\theta$)},\n")
        f.write("      ylabel={Average latency (ms)},\n")
        f.write("      xmin=0, xmax=1,\n")
        f.write(f"     ymin={0:.2f}, ymax={ymax:.2f},\n")
        f.write("      xtick={0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1},\n")
        f.write("      xticklabel style={/pgf/number format/fixed, /pgf/number format/precision=1},\n")
        f.write("      legend pos=outer north east,\n")
        f.write("      cycle list name=color list,\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocol_order):
            col = color_cycle[idx % len(color_cycle)]
            f.write(f"      \\addplot+[{col}, mark=*, thick] table {{\n")
            for x, y in zip(x_values, data_by_protocol[proto]):
                if y is None:
                    # skip missing points to create gaps
                    continue
                f.write(f"        {x:.2f} {y:.2f}\n")
            f.write("      };\n")
            f.write(f"      \\addlegendentry{{{proto}}}\n\n")

        # Draw a single horizontal dashed gray line for the average theoretical optimum across all locations
        if replica_means:
            avg_optimum = sum(replica_means) / len(replica_means)
            # draw a horizontal line from x=0 to x=1 at the average optimum value
            f.write(f"      \\addplot+[gray, dashed, thick] table {{\n")
            f.write(f"        0.00 {avg_optimum:.2f}\n")
            f.write(f"        1.00 {avg_optimum:.2f}\n")
            f.write("      };\n")
            f.write("      \\addlegendentry{optimum}\n\n")

        # include data replica location in caption (third entry in latencies.csv if present)
        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Average operation latency as a function of the ConflictWorkload parameter \\texttt{conflict.theta}. Each curve is a protocol; x-axis is the conflict rate (0 to 1), y-axis is average latency in ms.")
        # append the data replica info if available
        f.write(data_replica_caption)
        f.write("}\n")
        f.write("  \\label{fig:conflict-latency}\n")
        f.write("\\end{figure}\n")

if __name__ == "__main__":
    main()
