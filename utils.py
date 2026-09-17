#!/usr/bin/env python3
"""Shared helpers for the plotting scripts.

Everything here operates on the CSV produced by parse_ycsb_to_csv.sh, whose
schema is:

    protocol,nodes,workload,conflict_rate,dc,op,clients,tput,avg_latency_us,
    p1..p100,failed,fast_path,medium_path,slow_path,ephemeral_path
"""

import sys

import pandas as pd

# Columns that, when present, must hold a positive number for the row to be a
# measurement at all.  A run that timed out, crashed or completed nothing still
# leaves a row behind -- with a throughput that can even come out negative
# because the operation counter went below zero, or with every latency at 0
# after a minute of wall clock.
MEASUREMENT_COLUMNS = ("tput", "avg_latency_us", "p50")


def drop_unsound_rows(df, label=None, columns=MEASUREMENT_COLUMNS):
    """Return the rows of *df* that are valid measurements.

    A row is excluded when any of `columns` is present and does not hold a
    positive, finite number.  Values that simply cannot be parsed (YCSB writes
    "unknown" when a statistic is missing) are left alone: the callers already
    skip those, and absence of a number is not evidence of a failed run.

    The point is to keep such rows out of means, sums and medians, where they
    are indistinguishable from a slow-but-working system.  Exclusions are
    reported on stderr, because dropping data silently is its own hazard.
    """
    if df.empty:
        return df

    unsound = pd.Series(False, index=df.index)
    reasons = {}
    for col in columns:
        if col not in df.columns:
            continue
        value = pd.to_numeric(df[col], errors="coerce")
        bad = value.notnull() & ~(value > 0)
        if bad.any():
            reasons[col] = int(bad.sum())
        unsound |= bad

    if not unsound.any():
        return df

    prefix = "WARNING: " + (label + ": " if label else "")
    detail = ", ".join("%s<=0 in %d" % (c, n) for c, n in sorted(reasons.items()))
    print("%sexcluded %d of %d rows that are not measurements (%s)"
          % (prefix, int(unsound.sum()), len(df), detail), file=sys.stderr)

    # Name a few, so that a systematically broken configuration is visible
    # rather than just a count.
    identity = [c for c in ("protocol", "workload", "clients", "conflict_rate",
                            "nodes", "op", "dc") if c in df.columns]
    if identity:
        shown = df[unsound][identity].drop_duplicates().head(5)
        for _, row in shown.iterrows():
            print("%s  %s" % (prefix, " ".join("%s=%s" % (c, row[c]) for c in identity)),
                  file=sys.stderr)

    return df[~unsound].copy()
