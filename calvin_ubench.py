#!/usr/bin/env python3
"""
Plotting script for the Calvin micro-benchmark (Thomson et al., SIGMOD 2012, Figure 5).

This script generates one latency vs throughput graph per contention index (high
contention, CI=0.01, then low contention, CI=0.0001), side by side:
- X-axis: throughput (transactions/sec), summed over the data centers
- Y-axis: median latency (milliseconds), averaged over the data centers
- One line per protocol, one point per number of clients per site, showing the
  "hockey stick" effect where latency increases sharply and throughput
  plateaus/degrades as the system saturates
- Only the Pareto frontier of each line is kept: a point is cut when another
  point of the same protocol has at least its throughput and at most its latency
"""

import sys

import pandas as pd

from colors import (get_protocol_color, load_protocol_aliases, load_protocol_colors,
                    make_protocol_legend, sort_protocols_for_legend, sort_protocols_for_plotting)
from utils import drop_unsound_rows

# Contention indexes of Figure 5, from high to low contention
DEFAULT_CI_ORDER = [0.01, 0.0001]
CI_LABELS = {0.01: "High contention (CI=0.01)", 0.0001: "Low contention (CI=0.0001)"}


def usage_and_exit():
    print("Usage: python calvin_ubench.py results.csv output.tex")
    sys.exit(1)


def safe_int(x):
    try:
        return int(x)
    except (TypeError, ValueError):
        return None


def safe_float(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None


def pareto_frontier(points):
    """The (throughput, latency) points that no other point dominates, i.e.
    such that no other point has at least the same throughput and at most the
    same latency, sorted by throughput."""
    return sorted(p for p in set(points)
                  if not any(q != p and q[0] >= p[0] and q[1] <= p[1] for q in points))


def ci_label(ci):
    return CI_LABELS.get(ci, f"CI={ci:g}")


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]

    df = pd.read_csv(results_csv)
    df = drop_unsound_rows(df, label='calvin_ubench')

    # The transaction of the micro-benchmark
    df = df[df['op'] == 'tx-readmodifywrite'].copy()

    df['clients_int'] = df['clients'].apply(safe_int)
    df['tput_f'] = df['tput'].apply(safe_float)
    df['median_latency_ms'] = df['p50'].apply(safe_float)
    # The contention index is stored in the conflict_rate column
    df['ci'] = df['conflict_rate'].apply(safe_float)
    df = df[df['ci'].notnull() & df['clients_int'].notnull()
            & df['tput_f'].notnull() & df['median_latency_ms'].notnull()]
    if df.empty:
        print("Invalid data")
        sys.exit(1)

    raw_protocols = list(dict.fromkeys(df['protocol'].tolist()))
    protocol_order = sort_protocols_for_legend(raw_protocols)
    # For plotting, Accord is drawn last so its curve overwrites others.
    plot_order = sort_protocols_for_plotting(raw_protocols)
    present = sorted(df['ci'].unique().tolist(), reverse=True)
    ci_values = [ci for ci in DEFAULT_CI_ORDER if ci in present] + \
                [ci for ci in present if ci not in DEFAULT_CI_ORDER]

    # For each contention index and protocol, one (throughput, latency) point per
    # number of clients: the throughput is summed over the data centers and the
    # latency averaged over them.
    data = {}
    plotted_clients = set()
    for ci in ci_values:
        for proto in raw_protocols:
            subset = df[(df['protocol'] == proto) & (df['ci'] == ci)]
            points = []
            for clients in sorted(subset['clients_int'].unique().tolist()):
                rows = subset[subset['clients_int'] == clients]
                tput = rows['tput_f'].sum()
                lat = rows['median_latency_ms'].mean()
                if tput > 0 and lat > 0:
                    points.append((tput, lat))
                    plotted_clients.add(clients)
            data[(ci, proto)] = pareto_frontier(points)

    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[t]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocol_order, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\vspace{1mm}\\begin{tikzpicture}[scale=.7]\n")
        f.write("    \\begin{groupplot}[\n")
        f.write(f"      group style={{group size={len(ci_values)} by 1, horizontal sep=1.5cm}},\n")
        f.write("      width=7cm, height=5.5cm,\n")
        f.write("      grid=both,\n")
        f.write("      xlabel={Throughput (tx/sec)},\n")
        f.write("      tick label style={font=\\small},\n")
        f.write("      label style={font=\\small},\n")
        f.write("      title style={font=\\small},\n")
        f.write("      scaled x ticks=false,\n")
        f.write("    ]\n\n")

        for section, ci in enumerate(ci_values):
            points = [p for proto in raw_protocols for p in data[(ci, proto)]]
            xmax = max((t for t, _ in points), default=1000) * 1.1
            ymax = max((l for _, l in points), default=100) * 1.2
            f.write("      \\nextgroupplot[\n")
            f.write(f"        title={{{ci_label(ci)}}},\n")
            if section == 0:
                f.write("        ylabel={Median Latency (ms)},\n")
            f.write(f"        xmin=0, xmax={xmax:.2f},\n")
            f.write(f"        ymin=0, ymax={ymax:.2f},\n")
            f.write("      ]\n")
            for idx, proto in enumerate(plot_order):
                if not data[(ci, proto)]:
                    continue
                col = get_protocol_color(proto, protocol_colors, idx)
                f.write(f"      \\addplot+[{col}, mark=*, mark options=fill={col}, thick] table {{\n")
                for tput, lat in data[(ci, proto)]:
                    f.write(f"        {tput:.2f} {lat:.2f}\n")
                f.write("      };\n\n")

        f.write("    \\end{groupplot}\n")
        f.write("  \\end{tikzpicture}\n")
        # The range of clients per site tested, over every protocol
        clients_range = (f" from {min(plotted_clients)} up to {max(plotted_clients)}"
                         if plotted_clients else "")
        f.write("  \\caption{\\label{fig:calvin-ubench-latency} Calvin micro-benchmark:"
                f" latency vs throughput, increasing the number of clients per site{clients_range}.}}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
