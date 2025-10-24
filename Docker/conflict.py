#!/usr/bin/env python3
import sys
import pandas as pd
import numpy as np

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

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Average operation latency as a function of the ConflictWorkload parameter \\texttt{conflict.theta}. Each curve is a protocol; x-axis is the conflict rate (0 to 1), y-axis is average latency in ms.}\n")
        f.write("  \\label{fig:conflict-latency}\n")
        f.write("\\end{figure}\n")

if __name__ == "__main__":
    main()
    
