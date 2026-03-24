#!/usr/bin/env python3
"""
cockroachdb_breakdown.py - Parse CockroachDB YCSB trace logs and print a
latency breakdown (fast_commit, slow_commit, ordering, execution) per city.

With *db.tracing=true*, YCSB executes ``SET tracing = on`` before each
statement and ``SHOW TRACE FOR SESSION`` afterwards.  The trace rows are used
to decompose the end-to-end latency.

CockroachDB has no fast-path consensus, so fast_commit is always 0.
The remaining phases map to the fields produced by
_parse_one_cockroachdb_trace:
  slow_commit  <- commit   (total - processing - execution - ordering)
  ordering     <- ordering (Raft consensus duration)
  execution    <- execution (DistSQL / KV read phase)

Output: one CSV line per city to stdout:
  city,fast_commit,slow_commit,ordering,execution

Values are in microseconds, averaged over all traces found for the city.

Usage:
    python3 cockroachdb_breakdown.py <logdir> <workload> <nodes> <city1> [<city2> ...]

Example:
    python3 cockroachdb_breakdown.py logs/closed_economy ce 3 Hanoi Lyon NewYork
"""

import sys
import os
import re
import glob as glob_module
from datetime import datetime

# Regex to identify CockroachDB SHOW TRACE output lines
_TRACE_LINE_RE = re.compile(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\t')


# ---------------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------------

def is_trace_line(line):
    """Return True if *line* looks like a CockroachDB SHOW TRACE row."""
    return bool(_TRACE_LINE_RE.match(line))


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

    trace_lines = [line for line in lines if is_trace_line(line)]

    block_starts = []
    for idx, line in enumerate(trace_lines):
        cols = line.split('\t')
        if len(cols) < 6:
            continue
        msg = cols[2]
        span = cols[5]
        if '[NoTxn pos:' in msg and 'executing BindStmt' in msg and 'session recording' in span:
            block_starts.append(idx)

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

    Returns a dict {processing, execution, ordering, commit, total} in
    seconds, or None if any required event is missing or values are invalid.
    """
    t_start = None
    t_exec_start = None
    t_exec_end = None
    t_ord_start = None
    t_ord_end = None
    t_end = None

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
# File discovery and per-city breakdown
# ---------------------------------------------------------------------------

def find_log_files(logdir, nodes, workload, city):
    """
    Return all log files matching ``{logdir}/cockroachdb_{nodes}_{workload}_*_{city}.dat``.
    """
    pattern = os.path.join(logdir, f"cockroachdb_{nodes}_{workload}_*_{city}.dat")
    return sorted(glob_module.glob(pattern))


def compute_city_breakdown(logdir, nodes, workload, city):
    """
    Compute the average latency breakdown for the given city.

    Returns a dict {processing, execution, ordering, commit, total, n_traces}
    or None if no data are found.
    """
    files = find_log_files(logdir, nodes, workload, city)
    if not files:
        print(
            f"WARNING: No log file for cockroachdb/{city} "
            f"(workload={workload}, nodes={nodes})",
            file=sys.stderr,
        )
        return None

    all_traces = []
    for filepath in files:
        all_traces.extend(parse_cockroachdb_traces(filepath))

    if not all_traces:
        print(
            f"WARNING: No valid traces found for cockroachdb/{city} in {files}",
            file=sys.stderr,
        )
        return None

    n = len(all_traces)
    components = ['processing', 'execution', 'ordering', 'commit']
    avg = {comp: sum(t[comp] for t in all_traces) / n for comp in components}
    avg['total'] = sum(t['total'] for t in all_traces) / n
    avg['n_traces'] = n
    return avg


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main():
    if len(sys.argv) < 5:
        print(
            "Usage: python3 cockroachdb_breakdown.py <logdir> <workload> <nodes> <city1> [<city2> ...]",
            file=sys.stderr,
        )
        sys.exit(1)

    logdir = sys.argv[1]
    workload = sys.argv[2]
    try:
        nodes = int(sys.argv[3])
    except ValueError:
        print(f"ERROR: nodes must be an integer, got '{sys.argv[3]}'", file=sys.stderr)
        sys.exit(1)
    cities = sys.argv[4:]

    for city in cities:
        bd = compute_city_breakdown(logdir, nodes, workload, city)
        if bd is None:
            continue
        # Convert seconds to microseconds to match cassandra_breakdown.sh units.
        # fast_commit is always 0 (CockroachDB has no fast-path consensus).
        # slow_commit equals the commit time since fast_commit is 0.
        fast_commit = 0
        slow_commit = bd['commit'] * 1e6
        commit = slow_commit
        ordering = bd['ordering'] * 1e6
        execution = bd['execution'] * 1e6
        print(f"{city},{fast_commit},{slow_commit:.2f},{commit:.2f},{ordering:.2f},{execution:.2f}")


if __name__ == "__main__":
    main()
