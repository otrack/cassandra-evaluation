#!/usr/bin/env python3
"""
Plotting script for the YCSB throughput experiment.

Generates a grouped bar chart:
  - X-axis: YCSB workloads (A, B, C, D), one group per workload
  - Y-axis: total throughput (commands/sec) summed across all YCSB clients
  - One bar per protocol within each group
  - The first group's x-tick label lists the protocol names in small text
    above the workload letter; the remaining groups show only the workload
    letter.  All workload letters are horizontally aligned.
  - No legend box in the figure; protocols are identified in the caption
    via colour swatches.
"""

import sys
import pandas as pd


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

    # Total throughput per (protocol, workload): sum across all nodes/cities.
    # The 'tput' column is the same for every op-row that came from the same
    # measurement file, so deduplicate by (protocol, workload_upper, city)
    # before summing.
    df_unique = df.drop_duplicates(subset=['protocol', 'workload_upper', 'city'])
    tput_totals = (
        df_unique.groupby(['protocol', 'workload_upper'])['tput_f']
        .sum()
    )

    data = {}
    for workload in workloads:
        data[workload] = {}
        for proto in protocol_order:
            try:
                data[workload][proto] = float(tput_totals.loc[proto, workload])
            except KeyError:
                data[workload][proto] = 0.0

    # Colours consistent with the other plotting scripts in this repository
    color_cycle = [
        "red", "blue", "green!50!black", "cyan!80!black",
        "magenta!80!black", "yellow!80!black", "black"
    ]

    # Y-axis upper bound
    all_vals = [v for wl in data.values() for v in wl.values() if v > 0]
    ymax = max(all_vals) * 1.2 if all_vals else 1000.0

    # Build x-tick labels.
    # For the first workload group: protocol names in \tiny on the top line,
    # workload letter on the bottom line.  All other groups: a \phantom line
    # (same height as the protocol-name line) plus the workload letter so that
    # every workload letter sits at the same vertical position.
    phantom_ref = protocol_order[0] if protocol_order else "accord"
    proto_names_line = "~".join("\\tiny " + p for p in protocol_order)

    first_label = "\\shortstack[c]{" + proto_names_line + "\\\\" + workloads[0] + "}"
    phantom_line = "\\phantom{\\tiny " + phantom_ref + "}"
    other_labels = [
        "\\shortstack[c]{" + phantom_line + "\\\\" + w + "}"
        for w in workloads[1:]
    ]
    all_tick_labels = [first_label] + other_labels
    xticklabels_str = "{" + ",".join(all_tick_labels) + "}"

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      ybar,\n")
        f.write("      width=12cm, height=7cm,\n")
        f.write("      enlarge x limits=0.2,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      ylabel={Throughput (commands/sec)},\n")
        f.write("      symbolic x coords={" + ",".join(workloads) + "},\n")
        f.write("      xtick=data,\n")
        f.write(f"      xticklabels={xticklabels_str},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      legend style={draw=none, fill=none},\n")
        f.write("    ]\n\n")

        for idx, proto in enumerate(protocol_order):
            col = color_cycle[idx % len(color_cycle)]
            f.write(f"      \\addplot+[ybar, fill={col}, draw=black, forget plot] coordinates {{\n")
            for workload in workloads:
                val = data[workload].get(proto, 0.0)
                f.write(f"        ({workload}, {val:.2f})\n")
            f.write("      };\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")

        # Caption: describe the figure and identify protocols via colour swatches
        workloads_str = ", ".join(workloads)
        f.write(f"  \\caption{{Total throughput (summed across all YCSB clients) for workloads {workloads_str}. ")
        f.write("For each workload, one bar per protocol: ")
        for proto_idx, proto in enumerate(protocol_order):
            col = color_cycle[proto_idx % len(color_cycle)]
            f.write(
                r"\protect\tikz \protect\draw[thick, {color}] (0,0) -- +(0.8,0);~{{{proto}}}".format(
                    color=col, proto=proto
                )
            )
            if proto_idx < len(protocol_order) - 1:
                f.write(", ")
        f.write(".}\n")
        f.write("  \\label{fig:ycsb-throughput}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
