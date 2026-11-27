#!/usr/bin/env python3
"""
Plotting script for the Calvin micro-benchmark (Thomson et al., SIGMOD 2012, Figure 5).

This script generates one latency vs throughput graph per contention index (high
contention, CI=0.01, then low contention, CI=0.0001), side by side:
- X-axis: throughput (transactions/sec), summed over the data centers
- Y-axis: median latency (milliseconds), averaged over the data centers, on a log scale;
  --latency=pN plots the N-th percentile instead (e.g. --latency=p99), which shows
  saturation that only reaches the tail (a pinned lease holder keeps the median flat)
- One line per protocol, one point per number of clients per site, showing the
  "hockey stick" effect where latency increases sharply and throughput
  plateaus/degrades as the system saturates
- Every point is plotted, in the order of the number of clients, to spot anomalies; with
  --pareto, only the Pareto frontier of each line is kept: a point is cut when another
  point of the same protocol has at least its throughput and at most its latency

Given the results of the scale phase, it also generates the analogue of Figure 5,
one graph per contention index, with the total throughput (top) and the
throughput per node of a data center (bottom) against the number of nodes per
data center:
- One line per protocol and proportion of multipartition transactions (solid:
  the lowest proportion, dashed: the others), one point per number of nodes
- Each point is the best throughput over the numbers of clients tried
"""

import re
import sys

import pandas as pd

from colors import (get_protocol_color, load_protocol_aliases, load_protocol_colors,
                    make_protocol_legend, sort_protocols_for_legend, sort_protocols_for_plotting)
from utils import drop_unsound_rows

# Contention indexes of Figure 5, from high to low contention
DEFAULT_CI_ORDER = [0.01, 0.0001]
CI_LABELS = {0.01: "High contention (CI=0.01)", 0.0001: "Low contention (CI=0.0001)"}


def usage_and_exit():
    print("Usage: python calvin_ubench.py [--pareto] [--latency=pN] [--records=N] results.csv output.tex [scale_results.csv scale_output.tex]")
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


def latency_label(latency):
    return "Median" if latency == 'p50' else f"{latency[1:]}th Percentile"


def ci_label(ci):
    return CI_LABELS.get(ci, f"CI={ci:g}")


def main():
    pareto, latency, records = False, 'p50', None
    args = []
    for a in sys.argv[1:]:
        if a in ('--pareto', '-pareto'):
            pareto = True
        elif a.startswith('--latency='):
            latency = a.split('=', 1)[1]
            if not re.fullmatch(r'p([1-9][0-9]?|100)', latency):
                usage_and_exit()
        elif a.startswith('--records='):
            try:
                records = int(a.split('=', 1)[1])
            except ValueError:
                usage_and_exit()
        else:
            args.append(a)
    if len(args) not in (2, 4):
        usage_and_exit()

    plot_saturation(args[0], args[1], pareto, latency, records)
    if len(args) == 4:
        plot_scale(args[2], args[3])


def plot_saturation(results_csv, output_tikz, pareto=False, latency='p50', records=None):
    df = pd.read_csv(results_csv)
    df = drop_unsound_rows(df, label='calvin_ubench')

    # The transaction of the micro-benchmark
    df = df[df['op'] == 'tx-readmodifywrite'].copy()

    df['clients_int'] = df['clients'].apply(safe_int)
    df['tput_f'] = df['tput'].apply(safe_float)
    df['median_latency_ms'] = df[latency].apply(safe_float)
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
    curve_last = []  # the largest number of clients of each curve
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
                    last = clients
            if points:
                curve_last.append(last)
            data[(ci, proto)] = pareto_frontier(points) if pareto else points

    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[t]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocol_order, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\vspace{1mm}\\begin{tikzpicture}\n")
        f.write("    \\begin{groupplot}[\n")
        f.write(f"      group style={{group size={len(ci_values)} by 1, horizontal sep=1.2cm}},\n")
        # Each panel takes about half the text width: the plot, not the
        # caption, is what the figure is about.
        width = 0.95 / len(ci_values)
        f.write(f"      width={width:.2f}\\textwidth, height=0.42\\textwidth,\n")
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
            ymax = max((l for _, l in points), default=100) * 1.5
            ymin = min((l for _, l in points), default=10) / 1.5
            f.write("      \\nextgroupplot[\n")
            f.write(f"        title={{{ci_label(ci)}}},\n")
            if section == 0:
                f.write(f"        ylabel={{{latency_label(latency)} Latency (ms)}},\n")
            f.write(f"        xmin=0, xmax={xmax:.2f},\n")
            f.write(f"        ymode=log, ymin={ymin:.2f}, ymax={ymax:.2f},\n")
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
        clients_range = sweep_text(sorted(plotted_clients), curve_last)
        caption, note = saturation_caption(df, ci_values, latency, clients_range, pareto, records)
        f.write("  \\caption{\\label{fig:calvin-ubench-latency}" + caption + "}\n")
        # A parbox, so that the note is justified like the caption, not centered
        f.write("  \\par\\vspace{1mm}\\parbox{\\textwidth}{\\small\\color{gray}" + note + "}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


def _single(df, column):
    """The value of *column* when every row shares it, else None."""
    if column not in df.columns:
        return None
    values = pd.to_numeric(df[column], errors='coerce').dropna().unique()
    return values[0] if len(values) == 1 else None


def deployment_text():
    """Where the experiment ran, e.g. "AWS m5.4xlarge spot instances", or ""
    for the simulation (or when the configuration cannot be read)."""
    try:
        import infra
        provider = infra.provider()
        if provider == "simulation":
            return ""
        cfg = infra.load_config()
        machine = infra.machine_shape()
    except Exception:
        return ""
    name = {"aws": "AWS", "gcp": "GCP"}.get(provider, provider)
    spot = str(cfg.get(f"{provider}.spot", "")).lower() in ("1", "true", "yes")
    kind = "spot instances" if spot else "instances"
    return f"{name} {machine} {kind}" if machine else f"{name} {kind}"


def sweep_text(clients, curve_last):
    """How the numbers of clients per site were swept, for the caption.

    The sweeps stop once a system is overloaded, so the curves end at different
    numbers of clients: say so, rather than quoting the overall maximum as if
    every system had been run up to it."""
    if not clients:
        return ""
    text = f" One point per number of closed-loop clients per site, from {clients[0]}"
    ratios = {b / a for a, b in zip(clients, clients[1:])}
    if len(ratios) == 1:
        ratio = ratios.pop()
        text += " and doubling" if ratio == 2 else f", multiplied by {ratio:g} at each step"
    lo, hi = min(curve_last), max(curve_last)
    if lo == hi:
        return text + f", up to {hi}."
    return (text + "; each sweep stops once its system is overloaded,"
            f" so the curves end between {lo} and {hi} clients.")


def saturation_caption(df, ci_values, latency, clients_range, pareto, records=None):
    r"""The caption of the saturation plot, and a note on the micro-benchmark
    itself, set below it in gray.

    The key facts (what is plotted, the deployment, the axes and how to read
    the curves) are in bold.  The deployment and workload figures are given
    only when the results agree on them."""
    dcs, npd, mp = _single(df, 'nodes'), _single(df, 'partitions'), _single(df, 'mp')
    machines = deployment_text()
    setup = ""
    if dcs is not None:
        nodes = (rf"{int(dcs)} data centers $\times$ {int(npd)} partitions"
                 if npd is not None else f"{int(dcs)} data centers")
        setup = rf" on \textbf{{{nodes}}}"
        if machines:
            total = f"{int(dcs * npd)} " if npd is not None else ""
            setup += rf" ({total}\textbf{{{machines}}})"
    elif machines:
        setup = rf" on \textbf{{{machines}}}"
    if records:
        # records is YCSB's recordcount, i.e. over all the partitions
        if npd:
            setup += (rf", \textbf{{{int(records / npd):,} records per partition}}"
                      f" ({records:,} in total)")
        else:
            setup += rf", \textbf{{{records:,} records}}"

    positions = {1: [""], 2: ["left", "right"]}.get(
        len(ci_values), [f"panel {i}" for i in range(1, len(ci_values) + 1)])
    panels = [rf"\textbf{{{'high' if ci >= 0.01 else 'low'}}} ({pos + ', ' if pos else ''}CI={ci:g})"
              for pos, ci in zip(positions, ci_values)]
    panels = " and ".join([", ".join(panels[:-1]), panels[-1]] if len(panels) > 1 else panels)
    panels = rf", at {panels} \textbf{{contention}}" if panels else ""
    lat_text = "median" if latency == 'p50' else latency
    caption = (
        r"\textbf{Calvin micro-benchmark: latency vs.\ throughput}" + setup + panels + "."
        + clients_range +
        r" \textbf{x}: throughput summed over the sites;"
        rf" \textbf{{y}} (log scale): \textbf{{{lat_text} latency}} averaged over the sites."
        r" A curve \textbf{saturates} where it turns up and is \textbf{overloaded}"
        " where it bends back to the left."
        + (" Pareto frontier of each curve only." if pareto else "")
    )
    mp_text = (rf"{mp * 100:g}\% of them are multipartition"
               if mp is not None else "some of them are multipartition")
    note = (
        r"Micro-benchmark of Thomson et al.\ (SIGMOD 2012, Figure 5)."
        " A transaction reads counters, checks that their sum is non-negative and"
        " increments them: 10 records of one partition, or 5 on each of 2 partitions"
        f" for a multipartition one; {mp_text}."
        " One record per partition is hot, drawn among 1/CI hot records, so CI is the"
        " probability that two transactions conflict on it."
    )
    return caption, note


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
