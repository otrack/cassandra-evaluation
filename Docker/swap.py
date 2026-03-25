#!/usr/bin/env python3
"""
Plotting script for the swap workload experiment.

This script generates a line chart showing:
- X-axis: number of swapped items per transaction (parameter S), from 3 to 8
- Y-axis: median latency (ms) — the mean of p50 values across all nodes/cities
- One line per protocol (accord and cockroachdb)
- No legend (shared with the accompanying closed_economy figure)
"""

import sys
import pandas as pd
import numpy as np

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, sort_protocols_for_plotting


def usage_and_exit():
    print("Usage: python swap.py results.csv output.tex")
    sys.exit(1)


def row_median_latency(row):
    """Return the median latency (p50) from a DataFrame row."""
    v = row.get('p50', None)
    if pd.isna(v):
        return None
    if isinstance(v, str) and v.strip().lower() == "unknown":
        return None
    try:
        return float(v)
    except Exception:
        return None


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]

    df = pd.read_csv(results_csv)

    # Parse numeric columns
    def safe_int(x):
        try:
            return int(x)
        except Exception:
            return None

    # The conflict_rate column is reused to store swap.s values (extracted by parse_ycsb_to_csv.sh)
    df['s_val'] = df['conflict_rate'].apply(safe_int)

    # Keep only rows with valid S values
    df = df[df['s_val'].notnull()].copy()

    if df.empty:
        print("No valid swap workload data found in results CSV.")
        sys.exit(1)

    # Compute median latency per row from p50 percentile
    median_lats = []
    for idx, row in df.iterrows():
        median_lats.append(row_median_latency(row))
    df['median_latency_ms'] = median_lats
    df = df[df['median_latency_ms'].notnull()]

    if df.empty:
        print("No valid latency data found in results CSV.")
        sys.exit(1)

    # Get unique protocols sorted (accord last) for consistent plot draw order.
    raw_protocols = list(dict.fromkeys(df['protocol'].tolist()))
    protocol_order = sort_protocols_for_plotting(raw_protocols)

    # S values from 3 to 8
    s_values = sorted(df['s_val'].unique().tolist())

    # For each protocol, compute median latency per S value
    # (mean across all nodes/cities for the same S value)
    data_by_protocol = {}
    for proto in raw_protocols:
        dfp = df[df['protocol'] == proto]
        latencies = []
        for s in s_values:
            df_s = dfp[dfp['s_val'] == s]
            if not df_s.empty:
                latencies.append(float(df_s['median_latency_ms'].mean()))
            else:
                latencies.append(None)
        data_by_protocol[proto] = latencies

    # Prepare colors (unified protocol color schema)
    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    # Determine y-axis range
    all_lats = []
    for proto, vals in data_by_protocol.items():
        for v in vals:
            if v is not None:
                all_lats.append(v)

    if all_lats:
        ymin = 0
        ymax = max(all_lats) * 1.2
    else:
        ymin = 0
        ymax = 1000

    # Generate TikZ/pgfplots code
    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}[scale=.7]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=8cm, height=4cm,\n")
        f.write("      grid=both,\n")
        f.write("      xlabel={Number of swapped items ($S$)},\n")
        f.write("      ylabel={Median latency (ms)},\n")
        f.write(f"      xmin={min(s_values) - 0.5:.1f}, xmax={max(s_values) + 0.5:.1f},\n")
        f.write(f"      ymin={ymin:.2f}, ymax={ymax:.2f},\n")
        f.write("      xtick={" + ",".join(str(s) for s in s_values) + "},\n")
        f.write("      cycle list name=color list,\n")
        f.write("      tick label style={font=\\small},\n")
        f.write("      label style={font=\\small},\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocol_order):
            col = get_protocol_color(proto, protocol_colors, idx)
            f.write(f"      \\addplot+[{col}, mark=*, thick] table {{\n")
            for s, lat in zip(s_values, data_by_protocol[proto]):
                if lat is not None:
                    f.write(f"        {s} {lat:.2f}\n")
            f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Swap workload: median latency as a function of the number of swapped items per transaction ($S$).}\n")
        f.write("  \\label{fig:swap-latency}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
