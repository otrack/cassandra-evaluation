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

Given the results of the scale phase, it also generates the analogue of Figure 5,
one graph per contention index, with the total throughput (top) and the
throughput per node of a data center (bottom) against the number of nodes per
data center:
- One line per protocol and proportion of multipartition transactions (solid:
  the lowest proportion, dashed: the others), one point per number of nodes
- Each point is the best throughput over the numbers of clients tried
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
    print("Usage: python calvin_ubench.py results.csv output.tex [scale_results.csv scale_output.tex]")
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
    if len(sys.argv) not in (3, 5):
        usage_and_exit()

    plot_saturation(sys.argv[1], sys.argv[2])
    if len(sys.argv) == 5:
        plot_scale(sys.argv[3], sys.argv[4])


def plot_saturation(results_csv, output_tikz):
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
        print(f"No data in {results_csv}")
        return

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


def mp_label(mp):
    return f"{mp * 100:g}\\% MP"


def plot_scale(results_csv, output_tikz):
    df = pd.read_csv(results_csv)
    df = drop_unsound_rows(df, label='calvin_ubench_scale')
    df = df[df['op'] == 'tx-readmodifywrite'].copy()
    if 'partitions' not in df.columns:
        print(f"No partitions column in {results_csv}")
        return

    df['clients_int'] = df['clients'].apply(safe_int)
    df['tput_f'] = df['tput'].apply(safe_float)
    df['ci'] = df['conflict_rate'].apply(safe_float)
    df['npd'] = df['partitions'].apply(safe_int)
    df['mp_f'] = df['mp'].apply(safe_float)
    df = df[df['ci'].notnull() & df['clients_int'].notnull() & df['tput_f'].notnull()
            & df['npd'].notnull() & df['mp_f'].notnull()]
    if df.empty:
        print(f"No data in {results_csv}")
        return

    raw_protocols = list(dict.fromkeys(df['protocol'].tolist()))
    protocol_order = sort_protocols_for_legend(raw_protocols)
    plot_order = sort_protocols_for_plotting(raw_protocols)
    present = sorted(df['ci'].unique().tolist(), reverse=True)
    ci_values = [ci for ci in DEFAULT_CI_ORDER if ci in present] + \
                [ci for ci in present if ci not in DEFAULT_CI_ORDER]
    # With a single node per data center, every transaction is single-partition:
    # its runs belong to every line
    mp_values = sorted(df[df['npd'] > 1]['mp_f'].unique().tolist()) or \
        sorted(df['mp_f'].unique().tolist())
    npd_values = sorted(df['npd'].unique().tolist())

    # (ci, protocol, mp) -> [(nodes per DC, total throughput)], the throughput
    # being summed over the data centers and maximized over the clients
    data = {}
    for ci in ci_values:
        for proto in raw_protocols:
            for mp in mp_values:
                points = []
                for npd in npd_values:
                    rows = df[(df['protocol'] == proto) & (df['ci'] == ci) & (df['npd'] == npd)]
                    if npd > 1:
                        rows = rows[rows['mp_f'] == mp]
                    if rows.empty:
                        continue
                    best = rows.groupby('clients_int')['tput_f'].sum().max()
                    if best > 0:
                        points.append((npd, best))
                data[(ci, proto, mp)] = points

    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()
    styles = ["solid", "dashed", "dotted", "dashdotted"]

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[t]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocol_order, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\vspace{1mm}\\begin{tikzpicture}[scale=.7]\n")
        f.write("    \\begin{groupplot}[\n")
        f.write(f"      group style={{group size={len(ci_values)} by 2, horizontal sep=1.5cm, vertical sep=1.5cm}},\n")
        f.write("      width=7cm, height=5cm,\n")
        f.write("      grid=both,\n")
        f.write(f"      xtick={{{','.join(str(n) for n in npd_values)}}},\n")
        f.write("      tick label style={font=\\small},\n")
        f.write("      label style={font=\\small},\n")
        f.write("      title style={font=\\small},\n")
        f.write("      legend style={font=\\scriptsize},\n")
        f.write("      scaled y ticks=false,\n")
        f.write("      ymin=0,\n")
        f.write("    ]\n\n")

        for row, per_node in enumerate((False, True)):
            for section, ci in enumerate(ci_values):
                f.write("      \\nextgroupplot[\n")
                if row == 0:
                    f.write(f"        title={{{ci_label(ci)}}},\n")
                else:
                    f.write("        xlabel={Nodes per data center},\n")
                if section == 0:
                    label = "Throughput per node (tx/sec)" if per_node else "Total throughput (tx/sec)"
                    f.write(f"        ylabel={{{label}}},\n")
                f.write("      ]\n")
                for idx, proto in enumerate(plot_order):
                    col = get_protocol_color(proto, protocol_colors, idx)
                    for m, mp in enumerate(mp_values):
                        points = data[(ci, proto, mp)]
                        if not points:
                            continue
                        style = styles[m % len(styles)]
                        f.write(f"      \\addplot+[{col}, {style}, mark=*, mark options={{fill={col}, solid}}, thick]"
                                " table {\n")
                        for npd, tput in points:
                            f.write(f"        {npd} {tput / npd if per_node else tput:.2f}\n")
                        f.write("      };\n\n")

        f.write("    \\end{groupplot}\n")
        f.write("  \\end{tikzpicture}\n")
        mp_caption = ", ".join(f"{styles[m % len(styles)]}: {mp_label(mp)}" for m, mp in enumerate(mp_values))
        f.write("  \\caption{\\label{fig:calvin-ubench-scale} Calvin micro-benchmark:"
                f" total and per-node throughput, varying the number of nodes per data center"
                f" ({mp_caption} transactions).}}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
