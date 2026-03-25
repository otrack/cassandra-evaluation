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
        # A block starts either when BindStmt is in NoTxn state (cached prepared statement)
        # or when the workload PrepareStmt is in NoTxn state (first execution before cache).
        # SHOW TRACE FOR SESSION PrepareStmt is excluded to avoid false positives.
        if '[NoTxn pos:' in msg and 'session recording' in span and (
            'executing BindStmt' in msg or
            ('executing PrepareStmt' in msg and 'SHOW TRACE' not in msg)
        ):
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

    Key log lines used as boundaries
    ---------------------------------
    Processing start : ``[NoTxn pos:…] executing BindStmt`` (cached prepared stmt)
                       OR ``[NoTxn pos:…] executing PrepareStmt`` (first execution,
                       before the plan cache is populated; SHOW TRACE excluded)
    Processing end   : ``execution starts: distributed engine``
    Execution end    : ``writing batch with N requests``
                       (works for both single-row auto-commit and multi-row
                       explicit-transaction workloads such as ClosedEconomy)
    Ordering start   : ``node received request: N Put`` / ``EndTxn``
                       (write batch arriving at the leaseholder node)
    Ordering end     : ``ack-ing replication success``
    Request end      : first ``AutoCommit. err: <nil>`` after t_start
                       (covers both implicit auto-commit and explicit transaction
                       commit, and is always later than the ordering phase)

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

        # t_start: accept BindStmt (cached) OR the workload PrepareStmt (cold)
        if t_start is None and '[NoTxn pos:' in msg and (
                'executing BindStmt' in msg or
                ('executing PrepareStmt' in msg and 'SHOW TRACE' not in msg)):
            t_start = ts

        if t_exec_start is None and 'execution starts: distributed engine' in msg:
            t_exec_start = ts

        # t_exec_end: accept any number of requests (1 or more) and with or
        # without "and committing" (multi-row explicit-transaction workloads
        # such as ClosedEconomy write N rows without an inline commit here).
        if t_exec_end is None and 'writing batch with' in msg:
            t_exec_end = ts

        if t_ord_start is None and 'node received request:' in msg and (
                'Put' in msg or 'EndTxn' in msg):
            t_ord_start = ts

        if t_ord_end is None and 'ack-ing replication success' in msg:
            t_ord_end = ts

        # t_end: first AutoCommit after t_start.  This is correct for both
        # auto-commit single-statement workloads and explicit-transaction
        # workloads like ClosedEconomy where "execution ends" fires before the
        # Raft ordering phase.
        if t_end is None and t_start is not None and 'AutoCommit. err: <nil>' in msg:
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

def find_log_files(logdir, nodes, workload, city, protocol="cockroachdb"):
    """
    Return all log files matching ``{logdir}/{protocol}_{nodes}_{workload}_*_{city}.dat``.
    """
    pattern = os.path.join(logdir, f"{protocol}_{nodes}_{workload}_*_{city}.dat")
    return sorted(glob_module.glob(pattern))


def compute_city_breakdown(logdir, nodes, workload, city, protocol="cockroachdb"):
    """
    Compute the average latency breakdown for the given city.

    Returns a dict {processing, execution, ordering, commit, total, n_traces}
    or None if no data are found.
    """
    files = find_log_files(logdir, nodes, workload, city, protocol)
    if not files:
        print(
            f"WARNING: No log file for {protocol}/{city} "
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
    if len(sys.argv) < 6:
        print(
            "Usage: python3 cockroachdb_breakdown.py <protocol> <logdir> <workload> <nodes> <city1> [<city2> ...]",
            file=sys.stderr,
        )
        sys.exit(1)

    protocol = sys.argv[1]
    logdir = sys.argv[2]
    workload = sys.argv[3]
    try:
        nodes = int(sys.argv[4])
    except ValueError:
        print(f"ERROR: nodes must be an integer, got '{sys.argv[4]}'", file=sys.stderr)
        sys.exit(1)
    cities = sys.argv[5:]

    for city in cities:
        bd = compute_city_breakdown(logdir, nodes, workload, city, protocol)
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
