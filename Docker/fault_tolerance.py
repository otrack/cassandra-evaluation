#!/usr/bin/env python3
"""
Fault-tolerance experiment plotter.

Parses YCSB -s status output lines from log files produced by fault-tolerance.sh
and generates a throughput-over-time TikZ/pgfplots figure with one curve per protocol.

Usage:
    python3 fault_tolerance.py <logdir> <protocol1> [protocol2 ...] \
        <duration_s> <slowdown_s> <slowdown_end_s> <crash_s> <output.tex>
"""

import sys
import os
import re
import glob
from collections import defaultdict


def parse_status_lines(logfile):
    """
    Parse YCSB -s (status) output lines from a log file.

    YCSB prints a line every status.interval seconds of the form:
        ... N sec: M operations; K current ops/sec; ...

    Returns a list of (elapsed_seconds, current_ops_per_sec) tuples.
    """
    results = []
    try:
        with open(logfile, "r", errors="replace") as f:
            for line in f:
                m = re.search(
                    r"\b(\d+) sec: \d+ operations; ([\d.]+) current ops/sec", line
                )
                if m:
                    elapsed = int(m.group(1))
                    ops = float(m.group(2))
                    results.append((elapsed, ops))
    except OSError:
        pass
    return results


def main():
    # Expect: logdir protocol1 [protocol2 ...] duration_s slowdown_s slowdown_end_s crash_s output.tex
    if len(sys.argv) < 8:
        print(
            "Usage: fault_tolerance.py "
            "<logdir> <protocol1> [protocol2 ...] "
            "<duration_s> <slowdown_s> <slowdown_end_s> <crash_s> <output.tex>"
        )
        sys.exit(1)

    logdir = sys.argv[1]
    protocols = sys.argv[2:-5]
    duration_s = int(sys.argv[-5])
    slowdown_s = int(sys.argv[-4])
    slowdown_end_s = int(sys.argv[-3])
    crash_s = int(sys.argv[-2])
    output_tex = sys.argv[-1]

    color_cycle = [
        "red", "blue", "green!50!black", "cyan!80!black",
        "magenta!80!black", "yellow!80!black", "black"
    ]

    # Collect per-protocol aggregated throughput series
    protocol_data = {}
    for protocol in protocols:
        dat_files = glob.glob(os.path.join(logdir, f"{protocol}_*.dat"))
        if not dat_files:
            print(f"Warning: no .dat files found for protocol '{protocol}' in {logdir}")
            continue
        throughput_by_time = defaultdict(float)
        for f in dat_files:
            for elapsed, ops in parse_status_lines(f):
                throughput_by_time[elapsed] += ops
        if throughput_by_time:
            times = sorted(throughput_by_time.keys())
            protocol_data[protocol] = (times, [throughput_by_time[t] for t in times])

    if not protocol_data:
        print("No YCSB status lines found in any log files. Cannot plot.")
        sys.exit(0)

    all_throughputs = [v for _, tputs in protocol_data.values() for v in tputs]
    all_times = [t for times, _ in protocol_data.values() for t in times]
    ymax = max(all_throughputs) * 1.15
    xmax = max(max(all_times), duration_s)

    with open(output_tex, "w") as f:
        f.write("\\begin{figure}[htbp]\n")
        f.write("  \\centering\n")
        f.write("  \\begin{tikzpicture}\n")
        f.write("    \\begin{axis}[\n")
        f.write("      width=12cm, height=6cm,\n")
        f.write("      grid=both,\n")
        f.write("      xlabel={Time (seconds)},\n")
        f.write("      ylabel={Throughput (ops/sec)},\n")
        f.write(f"      xmin=0, xmax={xmax},\n")
        f.write(f"      ymin=0, ymax={ymax:.2f},\n")
        f.write("      legend pos=outer north east,\n")
        f.write("      legend style={font=\\small},\n")
        f.write("    ]\n\n")

        # One throughput curve per protocol
        for idx, protocol in enumerate(protocols):
            if protocol not in protocol_data:
                continue
            col = color_cycle[idx % len(color_cycle)]
            times, throughputs = protocol_data[protocol]
            f.write(f"      \\addplot[{col}, thick, mark=none] table {{\n")
            for t, tput in zip(times, throughputs):
                f.write(f"        {t} {tput:.2f}\n")
            f.write("      };\n")
            f.write(f"      \\addlegendentry{{{protocol}}}\n\n")

        # Vertical line: slowdown start (X/4)
        f.write(
            f"      \\addplot[orange, dashed, thick] "
            f"coordinates {{({slowdown_s},0) ({slowdown_s},{ymax:.2f})}};\n"
        )
        f.write("      \\addlegendentry{slowdown start}\n\n")

        # Vertical line: slowdown end (X/4+X/8)
        f.write(
            f"      \\addplot[orange!50!black, dashed, thick] "
            f"coordinates {{({slowdown_end_s},0) ({slowdown_end_s},{ymax:.2f})}};\n"
        )
        f.write("      \\addlegendentry{slowdown end}\n\n")

        # Vertical line: crash event (3X/4)
        f.write(
            f"      \\addplot[black, dashed, thick] "
            f"coordinates {{({crash_s},0) ({crash_s},{ymax:.2f})}};\n"
        )
        f.write("      \\addlegendentry{node crash}\n\n")

        f.write("    \\end{axis}\n")
        f.write("  \\end{tikzpicture}\n")
        protocols_str = ", ".join(protocols)
        f.write(
            "  \\caption{Aggregated YCSB throughput over time "
            f"({protocols_str}, {duration_s // 60}\\,min experiment). "
            f"At $t={slowdown_s}$\\,s a 400\\,ms latency is added to the first node "
            f"(bright orange dashed line); the slowdown is removed at $t={slowdown_end_s}$\\,s "
            f"(dark orange dashed line). "
            f"At $t={crash_s}$\\,s the first node is killed (black dashed line).}}\n"
        )
        f.write("  \\label{fig:fault-tolerance}\n")
        f.write("\\end{figure}\n")

    print(f"TikZ output written to {output_tex}")


if __name__ == "__main__":
    main()
