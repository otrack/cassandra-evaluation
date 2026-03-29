#!/usr/bin/env python3
"""
Plotting script for the swap workload experiment.

This script generates a two-subplot figure:
- Left: a line chart showing median latency (ms) vs. number of swapped items (S)
  with one line per protocol (accord and cockroachdb) for each client count (solid
  for 1 cl/site, dashed for multi-client).
- Right (optional): a stacked bar breakdown showing the latency breakdown per
  protocol for four groups: (#c=1,S=s_min), (#c=50,S=s_min), (#c=1,S=s_max),
  (#c=50,S=s_max).  Requires a breakdown.csv with a 'clients' column produced by
  swap.sh.
"""

import sys
import pandas as pd
import numpy as np

from colors import load_protocol_colors, load_protocol_aliases, get_protocol_color, sort_protocols_for_plotting, make_protocol_legend

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

    Returns a dict: {protocol: {clients: {S: {city: {fast_commit, slow_commit, commit, ordering, execution}}}}}
    Values are in microseconds.
    """
    result = {}
    try:
        df = pd.read_csv(breakdown_csv)
    except (FileNotFoundError, IOError):
        return result

    required = {'protocol', 'S', 'clients', 'city', 'fast_commit', 'slow_commit', 'commit', 'ordering', 'execution'}
    if not required.issubset(df.columns):
        return result

    for _, row in df.iterrows():
        proto = str(row['protocol']).strip()
        try:
            s_val = int(row['S'])
        except (TypeError, ValueError):
            continue
        try:
            clients_val = int(row['clients'])
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
        result.setdefault(proto, {}).setdefault(clients_val, {}).setdefault(s_val, {})[city] = {
            'fast_commit': fc,
            'slow_commit': sc,
            'commit': cm,
            'ordering': od,
            'execution': ex,
        }
    return result


def compute_average_swap_breakdown(breakdown_data, protocol, clients_val, s_val):
    """Return the average breakdown across all cities for (protocol, clients_val, s_val).

    Returns a dict {fast_commit, slow_commit, commit, ordering, execution} in
    microseconds, or None if no data are available.
    """
    cities_data = breakdown_data.get(protocol, {}).get(clients_val, {}).get(s_val, {})
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

    # Parse clients column to split single-client and multi-client runs
    df['clients_int'] = df['clients'].apply(safe_int)

    # Single-client subset (1 thread/DC)
    df_single = df[df['clients_int'] == 1].copy()
    if df_single.empty:
        # Fallback: treat all data as single-client when no clients=1 rows present
        df_single = df.copy()

    # Multi-client subset: auto-detect the largest non-1 client count
    non_one = df[df['clients_int'].notnull() & (df['clients_int'] != 1)]['clients_int']
    if not non_one.empty:
        multi_client_threads = int(non_one.max())
        df_multi = df[df['clients_int'] == multi_client_threads].copy()
    else:
        multi_client_threads = None
        df_multi = pd.DataFrame()
    has_multi = not df_multi.empty

    # Get unique protocols sorted (accord last) for consistent plot draw order.
    raw_protocols = list(dict.fromkeys(df_single['protocol'].tolist()))
    protocols_legend = sort_protocols_for_plotting(raw_protocols)
    protocol_order = sort_protocols_for_plotting(raw_protocols)

    # S values from 3 to 8
    s_values = sorted(df_single['s_val'].unique().tolist())

    # For each protocol, compute median latency per S value for single-client data
    # (mean across all nodes/cities for the same S value)
    data_by_protocol = {}
    for proto in raw_protocols:
        dfp = df_single[df_single['protocol'] == proto]
        latencies = []
        for s in s_values:
            df_s = dfp[dfp['s_val'] == s]
            if not df_s.empty:
                latencies.append(float(df_s['median_latency_ms'].mean()))
            else:
                latencies.append(None)
        data_by_protocol[proto] = latencies

    # For each protocol, compute median latency per S value for multi-client data
    data_by_protocol_multi = {}
    if has_multi:
        multi_protocols = list(dict.fromkeys(df_multi['protocol'].tolist()))
        for proto in multi_protocols:
            dfp = df_multi[df_multi['protocol'] == proto]
            latencies = []
            for s in s_values:
                df_s = dfp[dfp['s_val'] == s]
                if not df_s.empty:
                    latencies.append(float(df_s['median_latency_ms'].mean()))
                else:
                    latencies.append(None)
            data_by_protocol_multi[proto] = latencies

    # Prepare colors (unified protocol color schema)
    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()

    # Determine y-axis range covering both single- and multi-client data
    all_lats = []
    for proto, vals in data_by_protocol.items():
        for v in vals:
            if v is not None:
                all_lats.append(v)
    for proto, vals in data_by_protocol_multi.items():
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

    # Determine the available client counts and S values in the breakdown data
    bd_protocols = sort_protocols_for_plotting(list(breakdown_data.keys()))
    bd_proto_idx = {p: idx for idx, p in enumerate(bd_protocols)}

    # Collect all (clients, S) pairs that have at least one protocol with data
    bd_all_clients = set()
    bd_all_s = set()
    for proto in bd_protocols:
        for c in breakdown_data.get(proto, {}):
            bd_all_clients.add(c)
            for s in breakdown_data[proto][c]:
                bd_all_s.add(s)

    # Select s_min and s_max for the 4-group breakdown plot
    if bd_all_s:
        bd_s_min = min(bd_all_s)
        bd_s_max = max(bd_all_s)
    else:
        bd_s_min = None
        bd_s_max = None

    # Sort client counts; the problem specifies 1 and 50
    bd_clients_sorted = sorted(bd_all_clients)

    # Build the 4 groups: (clients, S) in the order
    #   (c_min, s_min), (c_max, s_min), (c_min, s_max), (c_max, s_max)
    # Each group contains one bar per available protocol.
    breakdown_groups = []    # list of (clients, s) group keys
    if bd_s_min is not None and len(bd_clients_sorted) >= 1:
        for s_sel in ([bd_s_min] if bd_s_min == bd_s_max else [bd_s_min, bd_s_max]):
            for c in bd_clients_sorted:
                # Only include group if at least one protocol has data for it
                has_data = any(
                    compute_average_swap_breakdown(breakdown_data, proto, c, s_sel) is not None
                    for proto in bd_protocols
                )
                if has_data:
                    breakdown_groups.append((c, s_sel))

    # Pre-compute breakdown averages for all (proto, clients, s) triples needed
    breakdown_avgs = {}   # {(proto, clients, s): avg_dict}
    for c, s in breakdown_groups:
        for proto in bd_protocols:
            avg = compute_average_swap_breakdown(breakdown_data, proto, c, s)
            if avg is not None:
                breakdown_avgs[(proto, c, s)] = avg

    has_breakdown = bool(breakdown_groups) and bool(breakdown_avgs)

    # Prepare colors from the unified protocol color schema
    protocol_colors = load_protocol_colors()
    protocol_aliases = load_protocol_aliases()
                
    # Generate TikZ/pgfplots code
    with open(output_tikz, 'w') as f:
        f.write("\\begin{figure}[t]\n")
        f.write("  \\centering\n")
        # Protocol legend in protocols.csv order
        f.write(make_protocol_legend(protocols_legend, protocol_colors,
                                     protocol_aliases=protocol_aliases))

        # Breakdown pattern legend placed before both tikzpictures so they
        # remain on the same line.
        if has_breakdown:
            bd_legend_entries = []
            for label, pattern in zip(BREAKDOWN_LABELS, BREAKDOWN_PATTERNS):
                swatch = (
                    r"\tiny\protect\tikz \protect\fill[fill=gray!40, pattern={pattern}, "
                    r" pattern color=gray!80] (0,0) rectangle (0.3,0.3);~{label}"
                ).format(pattern=pattern, label=label)
                bd_legend_entries.append(swatch)
            f.write("  " + "~".join(bd_legend_entries) + "\n\n")

        # ---- Left subplot: median latency line chart ----
        f.write("  \\vspace{1mm}\\begin{tikzpicture}[scale=.7]\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=7cm, height=4cm,\n")
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

        # Solid lines: single-client (1 thread/DC)
        for idx, proto in enumerate(protocol_order):
            col = get_protocol_color(proto, protocol_colors, idx)
            f.write(f"      \\addplot+[{col}, mark=*, thick] table {{\n")
            for s, lat in zip(s_values, data_by_protocol[proto]):
                if lat is not None:
                    f.write(f"        {s} {lat:.2f}\n")
            f.write("      };\n\n")

        # Dashed lines: multi-client (50 threads/DC)
        if has_multi:
            for idx, proto in enumerate(protocol_order):
                if proto not in data_by_protocol_multi:
                    continue
                col = get_protocol_color(proto, protocol_colors, idx)
                f.write(f"      \\addplot+[{col}, mark=*, thick, dashed] table {{\n")
                for s, lat in zip(s_values, data_by_protocol_multi[proto]):
                    if lat is not None:
                        f.write(f"        {s} {lat:.2f}\n")
                f.write("      };\n\n")

        f.write("    \\end{axis}\n")
        if has_multi:
            f.write(f"    \\node[font=\\tiny] at (2.25,3.2) {{1 cl/site (solid)}};\n")
            f.write(f"    \\node[font=\\tiny] at (2.5,2.75) {{{multi_client_threads} cl/site (dashed)}};\n")
        f.write("  \\end{tikzpicture}\n")

        # ---- Right subplot: stacked bar breakdown (4 groups) ----
        # Groups: (#c=1,S=s_min), (#c=50,S=s_min), (#c=1,S=s_max), (#c=50,S=s_max)
        # Within each group, one bar per protocol.
        if has_breakdown:
            # Assign sequential bar positions: for each group (c,s), one position
            # per protocol (in bd_protocols order).
            x_positions = {}   # {(proto, c, s): int position}
            xtick_pos = []
            xticklabels_list = []
            pos = 0
            for c, s in breakdown_groups:
                group_start = pos
                for proto in bd_protocols:
                    if (proto, c, s) in breakdown_avgs:
                        x_positions[(proto, c, s)] = pos
                        pos += 1
                group_mid = (group_start + pos - 1) / 2.0
            n_bars = pos

            # Compute ymax from the maximum total bar height across all items
            bd_totals = []
            for (proto, c, s), avgs in breakdown_avgs.items():
                total = sum(avgs.get(comp, 0.0) for comp in BREAKDOWN_COMPONENTS) / MICROS_TO_MILLIS
                bd_totals.append(total)
            breakdown_ymax = max(bd_totals) * 1.2 if bd_totals else 100.0

            xtick_str = ",".join(f"{x:.1f}" for x in xtick_pos)
            # Each label is wrapped in braces so that the commas inside labels are not
            # treated as list separators by pgfplots.
            xticklabels_str = ",".join("{" + lbl + "}" for lbl in xticklabels_list)

            f.write("  \\quad\\begin{tikzpicture}[scale=.6]\n")
            f.write("    \\begin{axis}[\n")
            f.write("      ybar stacked,\n")
            f.write("      width=5cm, height=5.5cm,\n")
            f.write("      enlarge x limits=0.1,\n")
            f.write("      bar width=0.2cm,\n")
            f.write("      ymajorgrids=true,\n")
            f.write("      ylabel={Median Latency (ms)},\n")
            f.write("      y label style={font=\\small},\n")
            f.write(f"      ymin=0, ymax={breakdown_ymax:.2f},\n")
            f.write(f"      xtick=\\empty,\n")
            f.write(f"      xticklabels={{{xticklabels_str}}},\n")
            f.write("      x label style={font=\\small, text width=1.4cm, align=center},\n")
            f.write("      tick label style={font=\\small},\n")
            f.write("      xlabel={breakdown},\n")
            f.write("    ]\n\n")

            # Emit one \addplot per (bar position, component).  Each \addplot lists
            # ALL bar positions so that pgfplots' per-x-value stack accumulator is
            # initialised for every position; non-relevant positions receive height 0.
            bar_items = sorted(x_positions.keys(), key=lambda t: x_positions[t])
            for proto, c, s in bar_items:
                proto_color = get_protocol_color(proto, protocol_colors, bd_proto_idx.get(proto, 0))
                bar_pos = x_positions[(proto, c, s)]
                for comp, pattern in zip(BREAKDOWN_COMPONENTS, BREAKDOWN_PATTERNS):
                    val = breakdown_avgs.get((proto, c, s), {}).get(comp, 0.0) / MICROS_TO_MILLIS
                    f.write(f"      \\addplot+[ybar, fill={proto_color}, draw=black,"
                            f" pattern={pattern}, pattern color={proto_color}] coordinates {{\n")
                    for j in range(n_bars):
                        if j == bar_pos:
                            f.write(f"        ({j}, {val:.3f})\n")
                        else:
                            f.write(f"        ({j}, 0)\n")
                    f.write("      };\n\n")

            f.write("    \\end{axis}\n")
            f.write(f"   \\node[font=\\tiny] at (1,-.25) {{$S$=3}};\n")
            f.write(f"   \\draw[dashed, line width=0.5pt] (1.7,-.25) -- (1.75,4.25);")            
            f.write(f"   \\node[font=\\tiny] at (2.5,-.25) {{$S$=8}};\n")
            f.write(f"   \\node[font=\\tiny] at (.3,2.45) {{1}};\n")
            f.write(f"   \\node[font=\\tiny] at (1.1,3.1) {{{multi_client_threads}}};\n")
            f.write("  \\end{tikzpicture}\n")

        f.write("  \\caption{Swap workload}\n"
        )
        f.write("  \\label{fig:swap-latency}\n")
        f.write("\\end{figure}\n")

    print(f"Generated {output_tikz}")


if __name__ == "__main__":
    main()

        # multi_caption = (
        #     f" Solid lines: 1 cl/site. Dashed lines: {multi_client_threads} cl/site."
        # ) if has_multi else ""
        # if has_breakdown:
        #     if bd_s_min == bd_s_max:
        #         s_range_str = f"$S={bd_s_min}$"
        #     else:
        #         s_range_str = f"$S={bd_s_min}$ and $S={bd_s_max}$"
        #         breakdown_caption = (
        #             " Right: Latency breakdown per protocol and client count"
        #             f" for {s_range_str},"
        #             " averaged across all sites."
        #         )
        # else:
        #     breakdown_caption = ""

        # f.write(
        #     "  \\caption{Left: Swap workload median latency as a function of the number of"
        #     " swapped items per transaction ($S$)." + multi_caption + breakdown_caption + "}\n"
        # )
