#!/usr/bin/env python3
"""
Plotting script for the Calvin micro-benchmark (Thomson et al., SIGMOD 2012, Figure 5).

This script generates a latency chart in the style of the closed economy one:
- X-axis: the protocols, in two sections separated by a dashed line, one per contention
  index (high contention, CI=0.01, then low contention, CI=0.0001)
- Y-axis: latency in milliseconds (log scale)
- For each protocol and contention index, a vertical range from the best to the worst
  latency, with markers for the median and the P90, P95 and P99 percentiles

Only the runs with the given number of clients per site are shown (by default, the largest
one in the results).  The latencies are averaged across the data centers.
"""

import sys

import numpy as np
import pandas as pd

from closed_economy import (LATENCY_METRICS, MARKER_METRICS, METRIC_COLUMNS, METRIC_MARKS,
                            estimate_row_latency, get_row_best_worst_latency, percentile_value)
from colors import (get_protocol_color, load_protocol_aliases, load_protocol_colors,
                    make_protocol_legend, sort_protocols_for_plotting)
from utils import drop_unsound_rows

# Contention indexes of Figure 5, from high to low contention
DEFAULT_CI_ORDER = [0.01, 0.0001]
CI_LABELS = {0.01: "high contention (CI=0.01)", 0.0001: "low contention (CI=0.0001)"}
SECTION_GAP = 1.0  # gap between two sections (holds the dashed separator)


def usage_and_exit():
    print("Usage: python calvin_ubench.py results.csv output.tex [threads]")
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


def ci_label(ci):
    return CI_LABELS.get(ci, f"CI={ci:g}")


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tikz = sys.argv[2]
    try:
        threads = int(sys.argv[3]) if len(sys.argv) >= 4 else None
    except ValueError:
        threads = None

    df = pd.read_csv(results_csv)
    df = drop_unsound_rows(df, label='calvin_ubench')

    # The transaction of the micro-benchmark
    df = df[df['op'] == 'tx-readmodifywrite'].copy()
    if df.empty:
        print("Invalid data")
        sys.exit(1)

    df['clients_int'] = df['clients'].apply(safe_int)
    # The contention index is stored in the conflict_rate column
    df['ci'] = df['conflict_rate'].apply(safe_float)
    df = df[df['ci'].notnull()]
    if threads is None:
        threads = int(df['clients_int'].dropna().max())
    df = df[df['clients_int'] == threads].copy()
    if df.empty:
        print(f"No data for {threads} clients per site")
        sys.exit(1)

    df['median_latency_ms'] = df.apply(estimate_row_latency, axis=1)
    df[['best_latency_ms', 'worst_latency_ms']] = df.apply(
        get_row_best_worst_latency, axis=1, result_type='expand'
    )
    for percentile in (90, 95, 99):
        df[f"p{percentile}_ms"] = df.apply(
            lambda row, p=percentile: percentile_value(row, p), axis=1
        )

    protocols = sort_protocols_for_plotting(df['protocol'].unique().tolist())
    present = sorted(df['ci'].unique().tolist(), reverse=True)
    ci_values = [ci for ci in DEFAULT_CI_ORDER if ci in present] + \
                [ci for ci in present if ci not in DEFAULT_CI_ORDER]

    # Average each metric across the data centers, per protocol and contention index
    data = {}
    for ci in ci_values:
        for proto in protocols:
            subset = df[(df['protocol'] == proto) & (df['ci'] == ci)]
            metrics = {}
            for metric in LATENCY_METRICS:
                vals = subset[METRIC_COLUMNS[metric]].dropna()
                metrics[metric] = float(np.mean(vals)) if not vals.empty else None
            data[(ci, proto)] = metrics

    vals = [v for m in data.values() for v in m.values() if v is not None and v > 0]
    ymin = min(vals) / 1.5 if vals else 1
    ymax = max(vals) * 1.5 if vals else 100

    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    n = len(protocols)
    section_width = n + SECTION_GAP
    xmin = -0.5
    xmax = (len(ci_values) - 1) * section_width + n - 0.5

    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[t]\n")
        f.write("  \\centering\n")
        f.write(make_protocol_legend(protocols, protocol_colors,
                                     protocol_aliases=protocol_aliases))
        f.write("  \\begin{tikzpicture}[scale=.6]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=7cm, height=5.5cm,\n")
        f.write("      grid=major,\n")
        f.write("      ymajorgrids=true,\n")
        f.write("      ymode=log,\n")
        f.write("      ylabel={Latency (ms)},\n")
        f.write(f"      ymin={ymin:.2f}, ymax={ymax:.2f},\n")
        f.write(f"      xmin={xmin:.2f}, xmax={xmax:.2f},\n")
        f.write("      xtick=\\empty,\n")
        f.write("      clip=false\n")
        f.write("    ]\n\n")

        for section, ci in enumerate(ci_values):
            base = section * section_width
            for proto_idx, proto in enumerate(protocols):
                col = get_protocol_color(proto, protocol_colors, proto_idx)
                metrics = data[(ci, proto)]
                avg_val = metrics.get("avg")
                if avg_val is None:
                    continue
                x = base + proto_idx
                best_val = metrics.get("best")
                worst_val = metrics.get("worst")
                if best_val is not None and worst_val is not None and best_val <= avg_val <= worst_val:
                    f.write(f"      \\addplot+[mark=-, color={col}, solid, forget plot] coordinates {{\n")
                    f.write(f"        ({x:.2f}, {best_val:.2f})\n")
                    f.write(f"        ({x:.2f}, {worst_val:.2f})\n")
                    f.write("      };\n\n")
                for metric in MARKER_METRICS:
                    val = metrics.get(metric)
                    if val is None:
                        continue
                    f.write(f"      \\addplot+[only marks, mark={METRIC_MARKS[metric]}, color={col},"
                            f" mark options=fill={col}, forget plot] coordinates {{\n")
                    f.write(f"        ({x:.2f}, {val:.2f})\n")
                    f.write("      };\n\n")

            # Section label below the axis
            label_x = base + (n - 1) / 2.0
            f.write(f"      \\node[font=\\tiny, anchor=north] at (axis cs:{label_x:.2f}, {ymin:.2f})"
                    f" {{{ci_label(ci)}}};\n")

            # Dashed separator between two sections
            if section + 1 < len(ci_values):
                sep_x = base + n - 0.5 + SECTION_GAP / 2.0
                f.write(f"      \\draw[dashed, gray] (axis cs:{sep_x:.2f}, {ymin:.2f})"
                        f" -- (axis cs:{sep_x:.2f}, {ymax:.2f});\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        f.write(f"  \\caption{{\\label{{fig:calvin-ubench-latency}} Calvin micro-benchmark"
                f" ({threads} cl/site)."
                " The markers indicate the median ($\\CIRCLE$), P90 ($\\blacktriangle$),"
                " P95 ($\\blacksquare$), and P99 ($\\blacklozenge$) percentiles.}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
