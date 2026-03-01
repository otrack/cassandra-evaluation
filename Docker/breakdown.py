#!/usr/bin/env python3
"""
cdf-breakdown.py - Parse protocol trace logs from YCSB benchmark runs and
produce a latency breakdown plot (processing / execution / ordering / commit)
per city for each trace-capable protocol.

Decomposition axes (mutually exclusive, summing to end-to-end latency):
  Processing : SQL planning / statement binding (or local key lookup for Accord)
  Execution  : DistSQL flow build + KV read + local compute (or Accord apply phase)
  Ordering   : latch acquisition + Raft consensus (or Accord PreAccept fast path)
  Commit     : network transport + apply/ack + response transfer
               (i.e. total - processing - execution - ordering)

Currently supported protocols: cockroachdb, accord

Usage:
    python3 cdf-breakdown.py <logdir> <workload1> [<workload2> ...] \
        <num_nodes> <city1> [<city2> ...] <output_prefix>

Example:
    python3 cdf-breakdown.py logs a 3 Hanoi Lyon NewYork results/cdf-breakdown
"""

import sys
import os
import re
import glob as glob_module
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime

# Protocols for which trace-based breakdown is implemented.
# TRACE_PARSERS (defined after the parser functions below) maps each protocol
# to its corresponding parse function.
TRACE_PROTOCOLS = {'cockroachdb', 'accord'}

# The four breakdown components (order matters for stacking)
COMPONENTS = ['processing', 'execution', 'ordering', 'commit']
COMPONENT_COLORS = ['#4e79a7', '#f28e2b', '#e15759', '#76b7b2']


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def is_trace_line(line):
    """Return True if *line* looks like a CockroachDB SHOW TRACE row."""
    return bool(re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\t', line))


def parse_timestamp(ts_str):
    """Parse a CockroachDB trace timestamp ('YYYY-MM-DD HH:MM:SS.SSSSSS')."""
    ts_str = ts_str.strip()
    try:
        return datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S.%f")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# CockroachDB trace parsing
# ---------------------------------------------------------------------------

def parse_cockroachdb_traces(filepath):
    """
    Parse all per-request traces from a CockroachDB YCSB log file.

    With *db.tracing=true*, YCSB executes ``SET tracing = on`` before each
    statement and ``SHOW TRACE FOR SESSION`` afterwards.  The trace rows
    (tab-separated) are interspersed with normal YCSB output in the file.

    Returns a list of dicts with float fields:
        processing, execution, ordering, commit, total  (all in seconds)
    """
    results = []

    try:
        with open(filepath, 'r', errors='replace') as fh:
            lines = fh.readlines()
    except OSError as exc:
        print(f"WARNING: Cannot read {filepath}: {exc}", file=sys.stderr)
        return results

    # Keep only lines that look like CockroachDB trace output
    trace_lines = [line for line in lines if is_trace_line(line)]

    # Each new request trace starts with a line that has both
    # '[NoTxn pos:' and 'executing BindStmt' in the 'session recording' span
    # (span column is index 5).
    block_starts = []
    for idx, line in enumerate(trace_lines):
        cols = line.split('\t')
        if len(cols) < 6:
            continue
        msg = cols[2]
        span = cols[5]
        if '[NoTxn pos:' in msg and 'executing BindStmt' in msg and 'session recording' in span:
            block_starts.append(idx)

    # Parse each block independently
    for bi, start_idx in enumerate(block_starts):
        end_idx = block_starts[bi + 1] if bi + 1 < len(block_starts) else len(trace_lines)
        block = trace_lines[start_idx:end_idx]
        result = _parse_one_cockroachdb_trace(block)
        if result is not None:
            results.append(result)

    return results


def _parse_one_cockroachdb_trace(lines):
    """
    Extract timing events from a single CockroachDB request trace block.

    Key log lines used as boundaries
    ---------------------------------
    Processing start : ``[NoTxn pos:…] executing BindStmt``
                       (first event visible to the client for this request)
    Processing end   : ``execution starts: distributed engine``
    Execution end    : ``writing batch with 1 requests and committing``
    Ordering start   : ``node received request: N Put`` / ``EndTxn``
                       (write batch arriving at the leaseholder node)
    Ordering end     : ``ack-ing replication success``
    Request end      : ``execution ends``

    Returns a dict {processing, execution, ordering, commit, total} in
    seconds, or None if any required event is missing or values are invalid.
    """
    t_start = None        # [NoTxn pos:] executing BindStmt
    t_exec_start = None   # execution starts: distributed engine
    t_exec_end = None     # writing batch with 1 requests and committing
    t_ord_start = None    # node received request: N Put / EndTxn
    t_ord_end = None      # ack-ing replication success
    t_end = None          # execution ends

    for line in lines:
        cols = line.split('\t')
        if len(cols) < 3:
            continue
        ts = parse_timestamp(cols[0])
        if ts is None:
            continue
        msg = cols[2].strip()

        if t_start is None and '[NoTxn pos:' in msg and 'executing BindStmt' in msg:
            t_start = ts

        if t_exec_start is None and 'execution starts: distributed engine' in msg:
            t_exec_start = ts

        if t_exec_end is None and 'writing batch with 1 requests and committing' in msg:
            t_exec_end = ts

        # The write batch (Put + EndTxn) is the one that goes through Raft
        if t_ord_start is None and 'node received request:' in msg and (
                'Put' in msg or 'EndTxn' in msg):
            t_ord_start = ts

        if t_ord_end is None and 'ack-ing replication success' in msg:
            t_ord_end = ts

        if 'execution ends' in msg:
            t_end = ts

    if None in (t_start, t_exec_start, t_exec_end, t_ord_start, t_ord_end, t_end):
        return None

    total = (t_end - t_start).total_seconds()
    processing = (t_exec_start - t_start).total_seconds()
    execution = (t_exec_end - t_exec_start).total_seconds()
    ordering = (t_ord_end - t_ord_start).total_seconds()
    commit = total - processing - execution - ordering

    if total <= 0 or processing < 0 or execution < 0 or ordering < 0 or commit < 0:
        return None

    return {
        'processing': processing,
        'execution': execution,
        'ordering': ordering,
        'commit': commit,
        'total': total,
    }


# ---------------------------------------------------------------------------
# Accord trace parsing
# ---------------------------------------------------------------------------

def _is_accord_event_line(line):
    """Return True if *line* is an Accord trace event (indented [ms] format)."""
    return bool(re.match(r'^\s+\[\d+\]', line))


def _accord_event_ms(line):
    """Extract the integer millisecond timestamp from '  [<ms>] ...'."""
    m = re.match(r'^\s+\[(\d+)\]', line)
    return int(m.group(1)) if m else None


def parse_accord_traces(filepath):
    """
    Parse all per-request Accord traces from a YCSB log file.

    With *db.tracing=true* and the Accord/Cassandra binding the driver collects
    a per-statement trace.  Each trace is emitted as::

        Trace ID: <uuid>, type: Execute CQL3 prepared query, duration: <N>us
          [<unix_ms>] <message> @ <node_ip>
          [<unix_ms>] <message> @ <node_ip>
          ...

    Lines that do not start with whitespace+bracket terminate the current block.

    Returns a list of dicts with float fields:
        processing, execution, ordering, commit, total  (all in seconds)
    """
    results = []

    try:
        with open(filepath, 'r', errors='replace') as fh:
            lines = fh.readlines()
    except OSError as exc:
        print(f"WARNING: Cannot read {filepath}: {exc}", file=sys.stderr)
        return results

    # Split into trace blocks: a block starts at "Trace ID:" and consists of
    # the subsequent indented "[ms]" event lines.
    blocks = []
    current_block = []
    in_trace = False

    for line in lines:
        if line.startswith('Trace ID:'):
            if current_block:
                blocks.append(current_block)
                current_block = []
            in_trace = True
        elif in_trace:
            if _is_accord_event_line(line):
                current_block.append(line)
            elif current_block:
                # A non-event line after some events → block is complete
                blocks.append(current_block)
                current_block = []
                in_trace = False
            # If current_block is still empty, stay in in_trace to handle
            # any header continuation lines before the first event line.

    if current_block:
        blocks.append(current_block)

    for block in blocks:
        result = _parse_one_accord_trace(block)
        if result is not None:
            results.append(result)

    return results


def _parse_one_accord_trace(lines):
    """
    Extract timing events from a single Accord trace block.

    Breakdown boundaries (all non-overlapping, summing to total)
    ------------------------------------------------------------
    Processing start : first event in trace (commands_for_key lookup)
    Processing end   : ``Local PreAccept for``
    Ordering end     : ``Local Execute for``   (covers consensus + post-fast-path gap)
    Execution end    : first ``Sending ACCORD_INFORM_DURABLE_REQ``
    Request end      : last event in trace

    commit = total − processing − ordering − execution

    Returns a dict {processing, execution, ordering, commit, total} in
    seconds, or None if any required event is missing or values are invalid.
    """
    ts_start = None
    ts_proc_end = None    # Local PreAccept for
    ts_ord_end = None     # Local Execute for
    ts_exec_end = None    # Sending ACCORD_INFORM_DURABLE_REQ
    ts_end = None         # last event

    for line in lines:
        ms = _accord_event_ms(line)
        if ms is None:
            continue

        stripped = line.strip()

        if ts_start is None:
            ts_start = ms
        ts_end = ms  # always update to last valid timestamp

        if ts_proc_end is None and 'Local PreAccept for' in stripped:
            ts_proc_end = ms

        if ts_ord_end is None and 'Local Execute for' in stripped:
            ts_ord_end = ms

        if ts_exec_end is None and 'Sending ACCORD_INFORM_DURABLE_REQ' in stripped:
            ts_exec_end = ms

    if None in (ts_start, ts_proc_end, ts_ord_end, ts_exec_end, ts_end):
        return None

    total = (ts_end - ts_start) / 1000.0
    processing = (ts_proc_end - ts_start) / 1000.0
    ordering = (ts_ord_end - ts_proc_end) / 1000.0
    execution = (ts_exec_end - ts_ord_end) / 1000.0
    commit = total - processing - ordering - execution

    if total <= 0 or processing < 0 or ordering < 0 or execution < 0 or commit < 0:
        return None

    return {
        'processing': processing,
        'execution': execution,
        'ordering': ordering,
        'commit': commit,
        'total': total,
    }


# Map protocol name → trace-parsing function
TRACE_PARSERS = {
    'cockroachdb': parse_cockroachdb_traces,
    'accord': parse_accord_traces,
}


# ---------------------------------------------------------------------------
# File discovery and per-city breakdown
# ---------------------------------------------------------------------------

def find_log_files(logdir, protocol, nodes, workload, city):
    """
    Return all log files matching ``{logdir}/{protocol}_{nodes}_{workload}_*_{city}.dat``.
    """
    pattern = os.path.join(logdir, f"{protocol}_{nodes}_{workload}_*_{city}.dat")
    return sorted(glob_module.glob(pattern))


def compute_city_breakdown(logdir, protocol, nodes, workload, city):
    """
    Compute the average latency breakdown for *protocol*/*city*.

    Returns a dict {processing, execution, ordering, commit, total,
    n_traces} or None if no data are found.
    """
    files = find_log_files(logdir, protocol, nodes, workload, city)
    if not files:
        print(
            f"WARNING: No log file for {protocol}/{city} "
            f"(workload={workload}, nodes={nodes})",
            file=sys.stderr,
        )
        return None

    if protocol not in TRACE_PROTOCOLS:
        print(f"WARNING: Tracing not supported for protocol '{protocol}'", file=sys.stderr)
        return None

    parser = TRACE_PARSERS[protocol]
    all_traces = []
    for filepath in files:
        all_traces.extend(parser(filepath))

    if not all_traces:
        print(
            f"WARNING: No valid traces found for {protocol}/{city} in {files}",
            file=sys.stderr,
        )
        return None

    n = len(all_traces)
    avg = {comp: sum(t[comp] for t in all_traces) / n for comp in COMPONENTS}
    avg['total'] = sum(t['total'] for t in all_traces) / n
    avg['n_traces'] = n
    return avg


# ---------------------------------------------------------------------------
# Plotting
# ---------------------------------------------------------------------------

def plot_breakdown(protocol, cities, breakdowns, output_prefix):
    """
    Generate a stacked bar chart of the latency breakdown for *protocol*.

    One bar per city; bars are stacked processing / execution / ordering / commit.
    Saves ``{output_prefix}_{protocol}.pdf`` (and ``.png``).
    """
    valid_cities = [c for c in cities if breakdowns.get(c) is not None]
    if not valid_cities:
        print(f"WARNING: No breakdown data for protocol '{protocol}'", file=sys.stderr)
        return

    x = np.arange(len(valid_cities))
    width = 0.5

    fig, ax = plt.subplots(figsize=(max(4, len(valid_cities) * 1.5 + 2), 5))

    bottoms = np.zeros(len(valid_cities))
    for comp, color in zip(COMPONENTS, COMPONENT_COLORS):
        values = np.array([breakdowns[c][comp] * 1000 for c in valid_cities])
        ax.bar(x, values, width, bottom=bottoms, label=comp.capitalize(), color=color)
        bottoms += values

    ax.set_xlabel('City')
    ax.set_ylabel('Average latency (ms)')
    ax.set_title(f'Latency breakdown — {protocol}')
    ax.set_xticks(x)
    ax.set_xticklabels(valid_cities)
    ax.legend(loc='upper right')
    ax.grid(axis='y', alpha=0.3)

    plt.tight_layout()
    out_pdf = f"{output_prefix}_{protocol}.pdf"
    out_png = f"{output_prefix}_{protocol}.png"
    plt.savefig(out_pdf)
    plt.savefig(out_png)
    plt.close()
    print(f"Saved breakdown plot: {out_pdf}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 6:
        print(
            "Usage: python3 cdf-breakdown.py <logdir> <workload1> [<workload2> ...] "
            "<num_nodes> <city1> [<city2> ...] <output_prefix>",
            file=sys.stderr,
        )
        sys.exit(1)

    logdir = sys.argv[1]
    remaining = sys.argv[2:]

    # Split remaining args into workloads / num_nodes / cities / output_prefix
    # using the same convention as cdf.py: the first integer is num_nodes.
    nodes = None
    nodes_idx = None
    for i, arg in enumerate(remaining):
        try:
            nodes = int(arg)
            nodes_idx = i
            break
        except ValueError:
            pass

    if nodes is None:
        print("ERROR: num_nodes (an integer) not found in arguments", file=sys.stderr)
        sys.exit(1)

    workloads = remaining[:nodes_idx]
    after_nodes = remaining[nodes_idx + 1:]

    if not workloads or len(after_nodes) < 2:
        print("ERROR: Must specify at least one workload, one city, and an output prefix",
              file=sys.stderr)
        sys.exit(1)

    # Last argument is the output prefix; everything before it are cities
    cities = after_nodes[:-1]
    output_prefix = after_nodes[-1]

    # Discover which trace-capable protocols have log files in logdir
    discovered = set()
    try:
        for fname in os.listdir(logdir):
            if not fname.endswith('.dat'):
                continue
            m = re.match(r'^([^_]+)_\d+_[^_]+_\d+_[A-Za-z]+\.dat$', fname)
            if m and m.group(1) in TRACE_PROTOCOLS:
                discovered.add(m.group(1))
    except OSError as exc:
        print(f"ERROR: Cannot list log directory '{logdir}': {exc}", file=sys.stderr)
        sys.exit(1)

    if not discovered:
        print(
            f"WARNING: No trace-capable protocol log files found in '{logdir}'",
            file=sys.stderr,
        )
        sys.exit(0)

    for protocol in sorted(discovered):
        print(f"Processing protocol: {protocol}")
        breakdowns = {}
        for city in cities:
            bd = None
            for workload in workloads:
                bd_w = compute_city_breakdown(logdir, protocol, nodes, workload, city)
                if bd_w is not None:
                    if bd is None:
                        bd = {comp: 0.0 for comp in COMPONENTS}
                        bd['total'] = 0.0
                        bd['n_traces'] = 0
                        bd['_count'] = 0
                    for comp in COMPONENTS:
                        bd[comp] += bd_w[comp]
                    bd['total'] += bd_w['total']
                    bd['n_traces'] += bd_w['n_traces']
                    bd['_count'] += 1
            if bd is not None and bd['_count'] > 0:
                for comp in COMPONENTS:
                    bd[comp] /= bd['_count']
                bd['total'] /= bd['_count']
                del bd['_count']
            breakdowns[city] = bd
            if bd:
                print(
                    f"  {city}: {bd['n_traces']} traces, "
                    f"avg total={bd['total']*1000:.1f}ms  "
                    f"processing={bd['processing']*1000:.1f}ms  "
                    f"execution={bd['execution']*1000:.1f}ms  "
                    f"ordering={bd['ordering']*1000:.1f}ms  "
                    f"commit={bd['commit']*1000:.1f}ms"
                )

        plot_breakdown(protocol, cities, breakdowns, output_prefix)


if __name__ == "__main__":
    main()
