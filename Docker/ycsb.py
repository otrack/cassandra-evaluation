#!/usr/bin/env python3
"""
Plotting script for the YCSB latency experiment.

Generates a grouped bar chart:
  - X-axis: YCSB workloads (A, B, C, D), one group per workload
  - Y-axis: median latency (ms) — the median of per-row p50 values across all
    executed operations and all clients for each workload/protocol group
  - One bar per protocol within each group, placed side-by-side
  - Each bar includes a standard deviation indicator (small solid horizontal
    line centred at the tip of the bar)
  - Y-axis fixed from 0 to 400 ms
  - Protocols ordered slowest → fastest (highest → lowest median latency)
  - Protocol name shown at the top of each bar in the first workload group
    only, rotated 45 degrees
  - No legend box in the figure; protocols are identified in the caption
    via colour swatches.
"""

import sys
import pandas as pd

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, make_protocol_legend, sort_protocols_for_legend, sort_protocols_for_plotting


def usage_and_exit():
    print("Usage: python ycsb.py results.csv workload1 [workload2 ...] num_nodes output.tex")
    sys.exit(1)


def main():
    # Expect: results.csv  w1 w2 ...  num_nodes  output.tex
    if len(sys.argv) < 5:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[-1]

    try:
        num_nodes = int(sys.argv[-2])
    except ValueError:
        usage_and_exit()

    workloads_lower = sys.argv[2:-2]
    if not workloads_lower:
        usage_and_exit()

    # Display workloads in upper-case (A, B, C, D)
    workloads = [w.upper() for w in workloads_lower]

    df = pd.read_csv(results_csv)

    # Filter by node count
    def safe_int(x):
        try:
            return int(x)
        except Exception:
            return None

    df['nodes_int'] = df['nodes'].apply(safe_int)
    df = df[df['nodes_int'] == num_nodes].copy()

    # Normalise workload column to upper-case for matching
    df['workload_upper'] = df['workload'].str.upper()
    df = df[df['workload_upper'].isin(workloads)]

    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    df['tput_f'] = df['tput'].apply(safe_float)
    df = df[df['tput_f'].notnull()]

    if df.empty:
        print("No valid data found in results CSV.")
        sys.exit(1)

    # Determine stable protocol order from data
    protocol_order = []
    for proto in df['protocol'].unique():
        if proto not in protocol_order:
            protocol_order.append(proto)

    # Median latency (ms) per (protocol, workload), computed from the p50 (median)
    # percentile column across all executed operations and all clients (cities).
    # p50 values are already in ms — no unit conversion needed.
    # CLEANUP rows are already excluded by parse_ycsb_to_csv.sh, but filter
    # defensively.
    df_lat = df[df['op'].str.lower() != 'cleanup'].copy()
    df_lat['p50_f'] = df_lat['p50'].apply(safe_float)
    df_lat = df_lat[df_lat['p50_f'].notnull()]

    if df_lat.empty:
        print("No valid p50 latency data found in results CSV.")
        sys.exit(1)

    # Median and standard deviation of p50 values per (protocol, workload).
    # p50 is the median latency in ms (no unit conversion needed).
    grp = df_lat.groupby(['protocol', 'workload_upper'])['p50_f']
    lat_medians = grp.median()
    lat_stds = grp.std().fillna(0)

    data = {}
    for workload in workloads:
        data[workload] = {}
        for proto in protocol_order:
            try:
                median = float(lat_medians.loc[proto, workload])
                std = float(lat_stds.loc[proto, workload])
            except KeyError:
                median = 0.0
                std = 0.0
            data[workload][proto] = (median, std)

    # Sort protocols by protocols.csv order for consistent legend and captions.
    legend_order = sort_protocols_for_legend(protocol_order)
    # For plotting, Accord is drawn last so its bars overwrite others visually.
    plot_order = sort_protocols_for_plotting(protocol_order)

    # Colours from the unified protocol color schema
    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    # Y-axis fixed at 0–400 ms as required
    ymax = 400.0

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[t]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(legend_order, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\begin{tikzpicture}[scale=.75]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      ybar,\n")
        f.write("      bar width=0.2cm,\n")
        f.write("      width=11cm, height=6cm,\n")
        f.write("      enlarge x limits=0.25,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      ylabel={Median latency (ms)},\n")
        f.write("      symbolic x coords={" + ",".join(workloads) + "},\n")
        f.write("      xtick=data,\n")
        f.write("      xticklabels={" + ",".join(workloads) + "},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      tick label style={font=\\small},\n")
        f.write("      label style={font=\\small},\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(plot_order):
            col = get_protocol_color(proto, protocol_colors, idx)
            f.write(f"      \\addplot+[fill={col}, draw=black,\n")
            f.write(f"        error bars/.cd, y dir=both, y explicit,\n")
            f.write(f"        error mark=-, error bar style={{solid, black, line width=0.8pt}},\n")
            f.write(f"      ] coordinates {{\n")
            for wl_idx, workload in enumerate(workloads):
                median, std = data[workload].get(proto, (0.0, 0.0))
                f.write(f"        ({workload}, {median:.2f}) +- (0, {std:.2f})\n")
            f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")

        # Caption: describe the figure without explicit colour swatches
        workloads_str = ", ".join(workloads)
        f.write(f"  \\caption{{Median operation latency (median across all clients and all executed operations) for YCSB workloads {workloads_str}. Error bars show the standard deviation.}}\n")
        f.write("  \\label{fig:ycsb-latency}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
