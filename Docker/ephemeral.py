#!/usr/bin/env python3
"""
Table generation script for the ephemeral reads experiment.

Reads a results CSV (produced by parse_ycsb_to_csv.sh) that contains two
protocol labels:

  - "accord"         : Accord with ephemeral reads ENABLED
  - "accord-noephem" : Accord with ephemeral reads DISABLED

Generates a LaTeX table showing, for each YCSB workload A–D:
  - Average operation latency with ephemeral reads ON (ms)
  - Average operation latency with ephemeral reads OFF (ms)
  - Speed-up = latency_off / latency_on
"""

import sys
import pandas as pd

PROTO_EPHEM_ON  = "accord"
PROTO_EPHEM_OFF = "accord-noephem"


def usage_and_exit():
    print("Usage: python3 ephemeral.py results.csv workload1 [workload2 ...] num_nodes output.tex")
    sys.exit(1)


def main():
    # Expect: results.csv  w1 w2 ...  num_nodes  output.tex
    if len(sys.argv) < 5:
        usage_and_exit()

    results_csv = sys.argv[1]
    output_tex  = sys.argv[-1]

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

    def safe_int(x):
        try:
            return int(x)
        except Exception:
            return None

    df['nodes_int'] = df['nodes'].apply(safe_int)
    df = df[df['nodes_int'] == num_nodes].copy()

    df['workload_upper'] = df['workload'].str.upper()
    df = df[df['workload_upper'].isin(workloads)]

    # Exclude CLEANUP rows
    df = df[df['op'].str.lower() != 'cleanup']

    def safe_float(x):
        try:
            return float(x)
        except Exception:
            return None

    df['avg_lat_f'] = df['avg_latency_us'].apply(safe_float)
    df = df[df['avg_lat_f'].notnull()]

    if df.empty:
        print("No valid data found in results CSV.")
        sys.exit(1)

    # Average latency (ms) per (protocol, workload), averaged over all
    # operations and all clients.
    lat_means = (
        df.groupby(['protocol', 'workload_upper'])['avg_lat_f']
        .mean() / 1000.0  # convert us -> ms
    )

    lat_on     = {}
    lat_off    = {}
    speedup    = {}

    for workload in workloads:
        try:
            on_val  = float(lat_means.loc[PROTO_EPHEM_ON,  workload])
        except KeyError:
            on_val  = None
        try:
            off_val = float(lat_means.loc[PROTO_EPHEM_OFF, workload])
        except KeyError:
            off_val = None

        lat_on[workload]  = on_val
        lat_off[workload] = off_val
        if on_val is not None and off_val is not None and on_val > 0:
            speedup[workload] = off_val / on_val
        else:
            speedup[workload] = None

    def fmt_lat(v):
        return f"{v:.1f}" if v is not None else "N/A"

    def fmt_speedup(v):
        return f"{v:.2f}$\\times$" if v is not None else "N/A"

    col_spec    = "l" + "c" * len(workloads)
    header_cols = " & ".join(f"{w}" for w in workloads)
    row_on_str  = " & ".join(fmt_lat(lat_on[w])   for w in workloads)
    row_off_str = " & ".join(fmt_lat(lat_off[w])  for w in workloads)
    row_sp_str  = " & ".join(fmt_speedup(speedup[w]) for w in workloads)
    workloads_str = ", ".join(workloads)

    with open(output_tex, 'w') as f:
        f.write("\\begin{table}[t]\n")
        f.write("  \\centering\n")
        f.write("  \\footnotesize\n")
        f.write(f"  \\begin{{tabular}}{{{col_spec}}}\n")
        f.write("    \\toprule\n")
        f.write(f"    & {header_cols} \\\\\n")
        f.write("    \\midrule\n")
        f.write(f"    Ephemeral ON (ms)  & {row_on_str}  \\\\\n")
        f.write(f"    Ephemeral OFF (ms) & {row_off_str} \\\\\n")
        f.write(f"    Speed-up           & {row_sp_str}  \\\\\n")
        f.write("    \\bottomrule\n")
        f.write("  \\end{tabular}\n")
        f.write(
            f"  \\caption{{Average operation latency\n"
            f"    and speed-up when ephemeral reads are enabled in \\Accord.}}\n"
             "  \\label{tab:ephemeral}\n"
        )
        f.write("\\end{table}\n")

    print(f"Generated {output_tex}")


if __name__ == "__main__":
    main()
