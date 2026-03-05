#!/usr/bin/env python3
"""
Plotting script for the YCSB latency experiment.

Generates a grouped bar chart:
  - X-axis: YCSB workloads (A, B, C, D), one group per workload
  - Y-axis: average latency (ms) averaged over all YCSB clients and all
    executed operations (read, insert, update, …) for each workload
  - One bar per protocol within each group, placed side-by-side
  - Protocols ordered slowest → fastest (highest → lowest average latency)
  - Protocol name shown at the top of each bar in the first workload group
    only, rotated 45 degrees
  - No legend box in the figure; protocols are identified in the caption
    via colour swatches.
"""

import sys
import pandas as pd

from colors import load_protocol_colors, get_protocol_color, make_protocol_legend


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

    # Average latency (ms) per (protocol, workload), averaged across all
    # executed operations and all clients (cities).
    # avg_latency_us is in microseconds; divide by 1000 to get ms.
    # CLEANUP rows are already excluded by parse_ycsb_to_csv.sh, but filter
    # defensively.
    df_lat = df[df['op'].str.lower() != 'cleanup'].copy()
    df_lat['avg_lat_f'] = df_lat['avg_latency_us'].apply(safe_float)
    df_lat = df_lat[df_lat['avg_lat_f'].notnull()]

    if df_lat.empty:
        print("No valid avg_latency_us data found in results CSV.")
        sys.exit(1)

    lat_means = (
        df_lat.groupby(['protocol', 'workload_upper'])['avg_lat_f']
        .mean() / 1000.0  # convert us → ms
    )

    data = {}
    for workload in workloads:
        data[workload] = {}
        for proto in protocol_order:
            try:
                data[workload][proto] = float(lat_means.loc[proto, workload])
            except KeyError:
                data[workload][proto] = 0.0

    # Sort protocols slowest → fastest (highest average latency first) so that
    # within each group the tallest bar is on the left.
    proto_avg_lat = {
        proto: sum(data[wl][proto] for wl in workloads) / (len(workloads) or 1)
        for proto in protocol_order
    }
    protocol_order.sort(key=lambda p: proto_avg_lat[p], reverse=True)

    # Colours from the unified protocol color schema
    protocol_colors = load_protocol_colors()

    # Y-axis upper bound — leave extra headroom for the rotated labels at bar tops
    LABEL_HEADROOM = 1.6
    all_vals = [v for wl in data.values() for v in wl.values() if v > 0]
    ymax = max(all_vals) * LABEL_HEADROOM if all_vals else 1000.0

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocol_order, protocol_colors))
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      ybar,\n")
        f.write("      bar width=0.35cm,\n")
        f.write("      width=14cm, height=8cm,\n")
        f.write("      enlarge x limits=0.25,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      ylabel={Average latency (ms)},\n")
        f.write("      symbolic x coords={" + ",".join(workloads) + "},\n")
        f.write("      xtick=data,\n")
        f.write("      xticklabels={" + ",".join(workloads) + "},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      every node near coord/.style={\n")
        f.write("        rotate=45, anchor=south west, inner sep=1pt,\n")
        f.write("        text=black, font=\\tiny,\n")
        f.write("      },\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocol_order):
            col = get_protocol_color(proto, protocol_colors, idx)
            f.write(f"      \\addplot+[fill={col}, draw=black,\n")
            f.write(f"        nodes near coords, point meta=explicit symbolic,\n")
            f.write(f"      ] coordinates {{\n")
            for wl_idx, workload in enumerate(workloads):
                val = data[workload].get(proto, 0.0)
                f.write(f"        ({workload}, {val:.2f}) []\n")
            f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")

        # Caption: describe the figure without explicit colour swatches
        workloads_str = ", ".join(workloads)
        f.write(f"  \\caption{{Average operation latency (averaged across all clients and all executed operations) for YCSB workloads {workloads_str}.}}\n")
        f.write("  \\label{fig:ycsb-latency}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
