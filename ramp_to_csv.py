#!/usr/bin/env python3
"""Converts the stages of a ramped run of the Calvin micro-benchmark (calvin.ramp)
into the rows of parse_ycsb_to_csv.sh, one per data center and stage, with the
number of threads of the stage as the number of clients.

Usage:
  ramp_to_csv.py <file.dat>...             CSV on the standard output
  ramp_to_csv.py --summary <file.dat>...   per stage: threads, tput (tx/s, summed
                                           over the files), mean p50 (ms)

A stage line of the workload is
  [RAMP], stage, threads, seconds, ops, failed, ops/s, avg us, p50 us, p95 us, p99 us
The CSV only has the percentiles that the workload reports (p50, p95, p99), in ms.
"""
import os
import re
import sys

FILE = re.compile(r'^([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_([A-Za-z]+)\.dat$')
OP = 'tx-readmodifywrite'


def parse(path):
    """Returns (properties of the run, stages), or None if the file has no stage."""
    m = FILE.match(os.path.basename(path))
    if not m:
        return None
    props = {'protocol': m.group(1), 'nodes': m.group(2), 'workload': m.group(3), 'dc': m.group(5)}
    for name, key in (('calvin.contentionindex', 'ci'), ('calvin.partitions', 'partitions'),
                      ('calvin.mpproportion', 'mp')):
        props[key] = 'NA'
    stages = []
    with open(path, errors='replace') as f:
        for line in f:
            if line.startswith('[RAMP],'):
                v = [x.strip() for x in line.split(',')[1:]]
                stages.append({'threads': int(v[1]), 'ops': int(v[3]), 'failed': int(v[4]),
                               'tput': float(v[5]), 'avg_us': float(v[6]),
                               'p50': int(v[7]), 'p95': int(v[8]), 'p99': int(v[9])})
                continue
            for name, key in (('calvin.contentionindex', 'ci'), ('calvin.partitions', 'partitions'),
                              ('calvin.mpproportion', 'mp')):
                if props[key] == 'NA':
                    mm = re.search(re.escape(name) + r'=([0-9.eE-]+)', line)
                    if mm:
                        props[key] = mm.group(1)
    return (props, stages) if stages else None


def header():
    cols = ['protocol', 'nodes', 'workload', 'conflict_rate', 'dc', 'op', 'clients', 'tput', 'avg_latency_us']
    cols += ['p%d' % p for p in range(1, 101)]
    cols += ['failed', 'fast_path', 'medium_path', 'slow_path', 'ephemeral_path', 'partitions', 'mp']
    return ','.join(cols)


def rows(props, stages):
    for s in stages:
        if s['ops'] == 0:
            continue
        pct = {50: s['p50'], 95: s['p95'], 99: s['p99']}
        lat = ['%d' % round(pct[p] / 1000) if p in pct else 'unknown' for p in range(1, 101)]
        failed = 100.0 * s['failed'] / (s['ops'] + s['failed'])
        yield ','.join([props['protocol'], props['nodes'], props['workload'], props['ci'], props['dc'], OP,
                        str(s['threads']), '%.2f' % s['tput'], '%.2f' % s['avg_us']] + lat
                       + ['%.4f' % failed, 'NA', 'NA', 'NA', 'NA', props['partitions'], props['mp']])


def main(argv):
    summary = argv[:1] == ['--summary']
    runs = [r for r in map(parse, argv[1:] if summary else argv) if r]
    if summary:
        by_stage = {}
        for _, stages in runs:
            for i, s in enumerate(stages):
                by_stage.setdefault(i, []).append(s)
        for i in sorted(by_stage):
            ss = by_stage[i]
            print(i, ss[0]['threads'], '%d' % round(sum(s['tput'] for s in ss)),
                  '%d' % round(sum(s['p50'] for s in ss) / len(ss) / 1000))
        return
    print(header())
    for props, stages in runs:
        for row in rows(props, stages):
            print(row)


if __name__ == '__main__':
    main(sys.argv[1:])
