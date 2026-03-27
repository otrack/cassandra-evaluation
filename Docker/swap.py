#!/usr/bin/env python3
"""
Plotting script for the swap workload experiment.

This script generates a two-subplot figure:
- Left: a line chart showing median latency (ms) vs. number of swapped items (S)
  with one line per protocol (accord and cockroachdb).
- Right (optional): a stacked bar breakdown showing the latency breakdown per
  protocol phase for each value of S.  Requires a breakdown.csv produced by
  swap.sh.
"""

import sys
import pandas as pd
import numpy as np

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, sort_protocols_for_plotting

BREAKDOWN_COMPONENTS = ['commit', 'ordering', 'execution']
BREAKDOWN_LABELS = ['Commit', 'Ordering', 'Execution']
BREAKDOWN_PATTERNS = ['north east lines', 'horizontal lines', 'north west lines']
MICROS_TO_MILLIS = 1000.0


def usage_and_exit():
    print("Usage: python swap.py results.csv [breakdown.csv] output.tex")
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


def load_swap_breakdown(breakdown_csv):
    """Load swap breakdown data from breakdown.csv.

    Returns a dict: {protocol: {S: {city: {fast_commit, slow_commit, commit, ordering, execution}}}}
    Values are in microseconds.
    """
    result = {}
    try:
        df = pd.read_csv(breakdown_csv)
    except (FileNotFoundError, IOError):
        return result

    required = {'protocol', 'S', 'city', 'fast_commit', 'slow_commit', 'commit', 'ordering', 'execution'}
    if not required.issubset(df.columns):
        return result

    for _, row in df.iterrows():
        proto = str(row['protocol']).strip()
        try:
            s_val = int(row['S'])
        except (TypeError, ValueError):
            continue
        city = str(row['city']).strip()
        try:
            fc = float(row['fast_commit'])
            sc = float(row['slow_commit'])
            cm = float(row['commit'])
            od = float(row['ordering'])
            ex = float(row['execution'])
        except (TypeError, ValueError):
            continue
        result.setdefault(proto, {}).setdefault(s_val, {})[city] = {
            'fast_commit': fc,
            'slow_commit': sc,
            'commit': cm,
            'ordering': od,
            'execution': ex,
        }
    return result


def compute_average_swap_breakdown(breakdown_data, protocol, s_val):
    """Return the average breakdown across all cities for (protocol, s_val).

    Returns a dict {fast_commit, slow_commit, commit, ordering, execution} in
    microseconds, or None if no data are available.
    """
    cities_data = breakdown_data.get(protocol, {}).get(s_val, {})
    if not cities_data:
        return None
    components = ['fast_commit', 'slow_commit', 'commit', 'ordering', 'execution']
    totals = {c: 0.0 for c in components}
    n = len(cities_data)
    for city_bd in cities_data.values():
        for c in components:
            totals[c] += city_bd.get(c, 0.0)
    return {c: totals[c] / n for c in components}


def main():
    if len(sys.argv) < 3:
        usage_and_exit()

    results_csv = sys.argv[1]
    # Optional breakdown.csv: present when 4 arguments are given
    if len(sys.argv) >= 4:
        breakdown_csv = sys.argv[2]
        output_tikz = sys.argv[3]
    else:
        breakdown_csv = None
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

    # Load and process breakdown data
    breakdown_data = {}
    if breakdown_csv is not None:
        breakdown_data = load_swap_breakdown(breakdown_csv)

    # Build ordered list of (protocol, s) pairs that have breakdown data.
    # Protocols are sorted in protocols.csv order (accord last).
    bd_protocols = sort_protocols_for_plotting(list(breakdown_data.keys()))
    bd_proto_idx = {p: idx for idx, p in enumerate(bd_protocols)}
    breakdown_items = []    # list of (proto, s)
    breakdown_avgs = {}     # {(proto, s): avg_dict}
    for s in s_values:
        for proto in bd_protocols:
            avg = compute_average_swap_breakdown(breakdown_data, proto, s)
            if avg is not None:
                breakdown_items.append((proto, s))
                breakdown_avgs[(proto, s)] = avg

    # Generate TikZ/pgfplots code
    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")

        # Breakdown pattern legend placed before both tikzpictures so they
        # remain on the same line.
        if breakdown_items:
            bd_legend_entries = []
            for label, pattern in zip(BREAKDOWN_LABELS, BREAKDOWN_PATTERNS):
                swatch = (
                    r"\protect\tikz \protect\fill[fill=gray!40, pattern={pattern},"
                    r" pattern color=gray!80] (0,0) rectangle (0.3,0.3);~{label}"
                ).format(pattern=pattern, label=label)
                bd_legend_entries.append(swatch)
            f.write("  " + "~".join(bd_legend_entries) + "\n\n")

        # ---- Left subplot: median latency line chart ----
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

        # ---- Right subplot: stacked bar breakdown ----
        if breakdown_items:
            # Assign sequential x positions grouped by S value (one group per S,
            # one bar per protocol within each group).
            s_groups = {}
            for proto, s in breakdown_items:
                s_groups.setdefault(s, []).append(proto)

            x_positions = {}
            pos = 0
            xtick_pos = []
            xticklabels_list = []
            for s in s_values:
                if s not in s_groups:
                    continue
                group_protos = s_groups[s]
                group_start = pos
                for proto in group_protos:
                    x_positions[(proto, s)] = pos
                    pos += 1
                group_mid = (group_start + pos - 1) / 2.0
                xtick_pos.append(group_mid)
                xticklabels_list.append(str(s))
            n_bars = pos

            # Compute ymax from the maximum total bar height across all items
            bd_totals = []
            for proto, s in breakdown_items:
                avgs = breakdown_avgs.get((proto, s), {})
                total = sum(avgs.get(c, 0.0) for c in BREAKDOWN_COMPONENTS) / MICROS_TO_MILLIS
                bd_totals.append(total)
            breakdown_ymax = max(bd_totals) * 1.2 if bd_totals else 100.0

            xtick_str = ",".join(f"{x:.1f}" for x in xtick_pos)
            xticklabels_str = ",".join(xticklabels_list)

            f.write("  \\begin{tikzpicture}[scale=.6]\n")
            f.write("    \\begin{axis}[\n")
            f.write("      ybar stacked,\n")
            f.write("      width=6cm, height=6cm,\n")
            f.write("      enlarge x limits=0.15,\n")
            f.write("      bar width=0.2cm,\n")
            f.write("      ymajorgrids=true,\n")
            f.write("      ylabel={Median Latency (ms)},\n")
            f.write(f"      ymin=0, ymax={breakdown_ymax:.2f},\n")
            f.write(f"      xtick={{{xtick_str}}},\n")
            f.write(f"      xticklabels={{{xticklabels_str}}},\n")
            f.write("      tick label style={font=\\tiny},\n")
            f.write("      xlabel={breakdown},\n")
            f.write("    ]\n\n")

            # Emit one \addplot per (bar position, phase).  Each \addplot lists
            # ALL bar positions so that pgfplots' per-x-value stack accumulator
            # is initialised for every position; non-relevant positions receive
            # height 0.  This guarantees that every bar starts at y=0 regardless
            # of the order \addplot commands are emitted.
            for i, (proto, s) in enumerate(breakdown_items):
                proto_color = get_protocol_color(proto, protocol_colors, bd_proto_idx.get(proto, i))
                bar_pos = x_positions[(proto, s)]
                for comp, pattern in zip(BREAKDOWN_COMPONENTS, BREAKDOWN_PATTERNS):
                    val = breakdown_avgs.get((proto, s), {}).get(comp, 0.0) / MICROS_TO_MILLIS
                    f.write(f"      \\addplot+[ybar, fill={proto_color}, draw=black,"
                            f" pattern={pattern}, pattern color={proto_color}] coordinates {{\n")
                    for j in range(n_bars):
                        if j == bar_pos:
                            f.write(f"        ({j}, {val:.3f})\n")
                        else:
                            f.write(f"        ({j}, 0)\n")
                    f.write("      };\n\n")

            f.write("    \\end{axis}\n")
            f.write("  \\end{tikzpicture}\n")

        breakdown_caption = (
            " Right: Latency breakdown per protocol phase for each value of $S$,"
            " averaged across all data centers."
        ) if breakdown_items else ""

        f.write(
            "  \\caption{Left: Swap workload median latency as a function of the number of"
            " swapped items per transaction ($S$)." + breakdown_caption + "}\n"
        )
        f.write("  \\label{fig:swap-latency}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()
