#!/usr/bin/env python3
"""
Plotting script for the closed economy experiment.

This script generates a grouped bar chart (histogram) showing:
- X-axis: number of nodes in the system (3, 4, 5)
- Y-axis: number of transactions (throughput in ops/sec)
- One bar group per node count, with bars for each protocol (accord, cockroachdb)

The plot style is similar to Fig. 9 from https://arxiv.org/pdf/2104.01142
"""

import sys
import pandas as pd
import numpy as np


def usage_and_exit():
    print("Usage: python closed_economy.py results.csv output.tex")
    sys.exit(1)


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]

    df = pd.read_csv(results_csv)

    # Filter for readmodifywrite operations (the transaction operation in closed economy)
    df_rmw = df[df['op'] == 'readmodifywrite'].copy()

    # If no readmodifywrite data, fall back to all data
    if df_rmw.empty:
        df_rmw = df.copy()

    # Parse nodes as integers
    def safe_int(x):
        try:
            return int(x)
        except Exception:
            return None

    df_rmw['nodes_int'] = df_rmw['nodes'].apply(safe_int)
    df_rmw = df_rmw[df_rmw['nodes_int'].notnull()]

    # Get unique protocols and node counts, sorted
    protocols = sorted(df_rmw['protocol'].unique().tolist())
    node_counts = sorted(df_rmw['nodes_int'].unique().tolist())

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
                for val in subset['tput']:
                    try:
                        tput_vals.append(float(val))
                    except Exception:
                        pass
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
                "as a function of the number of nodes. Each bar represents a protocol.}\n")
        f.write("  \\label{fig:closed-economy-throughput}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
