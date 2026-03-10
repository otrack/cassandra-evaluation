#!/usr/bin/env python3
"""
Plotting script for the swap workload experiment.

This script generates a line chart showing:
- X-axis: number of swapped items per transaction (parameter S), from 3 to 8
- Y-axis: total system throughput (ops/sec), summed across all nodes
- One line per protocol (accord and cockroachdb)
"""

import sys
import pandas as pd
import numpy as np

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, make_protocol_legend


def usage_and_exit():
    print("Usage: python swap.py results.csv output.tex")
    sys.exit(1)


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]

    df = pd.read_csv(results_csv)

    # Parse numeric columns
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
    # The conflict_rate column is reused to store swap.s values (extracted by parse_ycsb_to_csv.sh)
    df['s_val'] = df['conflict_rate'].apply(safe_int)

    # Keep only rows with valid S values and throughput
    df = df[df['tput_f'].notnull() & df['s_val'].notnull()].copy()

    if df.empty:
        print("No valid swap workload data found in results CSV.")
        sys.exit(1)

    # Get unique protocols in order of appearance
    protocol_order = []
    for proto in df['protocol'].unique():
        if proto not in protocol_order:
            protocol_order.append(proto)

    # S values from 3 to 8
    s_values = sorted(df['s_val'].unique().tolist())

    # For each protocol, compute total throughput per S value
    # (sum across all nodes/cities for the same S value)
    data_by_protocol = {}
    for proto in protocol_order:
        dfp = df[df['protocol'] == proto]
        throughputs = []
        for s in s_values:
            df_s = dfp[dfp['s_val'] == s]
            if not df_s.empty:
                total_tput = df_s['tput_f'].sum()
                throughputs.append(total_tput)
            else:
                throughputs.append(None)
        data_by_protocol[proto] = throughputs

    # Prepare colors (unified protocol color schema)
    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    # Determine y-axis range
    all_tputs = []
    for proto, vals in data_by_protocol.items():
        for v in vals:
            if v is not None:
                all_tputs.append(v)

    if all_tputs:
        ymin = 0
        ymax = max(all_tputs) * 1.2
    else:
        ymin = 0
        ymax = 1000

    # Generate TikZ/pgfplots code
    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocol_order, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\begin{tikzpicture}[scale=.7]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=8cm,\n")
        f.write("      grid=both,\n")
        f.write("      xlabel={Number of swapped items ($S$)},\n")
        f.write("      ylabel={Total throughput (ops/sec)},\n")
        f.write(f"      xmin={min(s_values) - 0.5:.1f}, xmax={max(s_values) + 0.5:.1f},\n")
        f.write(f"      ymin={ymin:.2f}, ymax={ymax:.2f},\n")
        f.write("      xtick={" + ",".join(str(s) for s in s_values) + "},\n")
        f.write("      cycle list name=color list,\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocol_order):
            col = get_protocol_color(proto, protocol_colors, idx)
            f.write(f"      \\addplot+[{col}, mark=*, thick] table {{\n")
            for s, tput in zip(s_values, data_by_protocol[proto]):
                if tput is not None:
                    f.write(f"        {s} {tput:.2f}\n")
            f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write("  \\caption{Swap workload: total system throughput as a function of the number of swapped items per transaction ($S$).}\n")
        f.write("  \\label{fig:swap-throughput}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
