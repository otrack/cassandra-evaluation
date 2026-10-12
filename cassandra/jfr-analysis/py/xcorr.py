#!/usr/bin/env python3
"""Cross-correlate slow request completions across nodes.

  RequestLatency (CSV=1) per node -> <dir>/<node>.csv, then:
  xcorr.py <dir> [Node1 Node2 ...]

For each node X with enough slow requests (> max(500ms, 3x its p50)), and every other node Y, histograms the
offset X_end - Y_end against Y's slow requests (> 1.8x Y's p50), normalised by a baseline of X's fast requests,
and prints the peak (10ms bins) next to the one-way delay X<->Y. Peaks at +one-way mean X's slow requests finish one
hop after Y's: they were released by an event at Y. ONE_WAY below holds the AWS one-way delays (ms) measured from
the durability-round stall propagation (Ashburn us-east-1, Frankfurt eu-central-1, Osasco sa-east-1, Singapore
ap-southeast-1, Tokyo ap-northeast-1); edit for other deployments.
"""
import csv, bisect, collections, sys, random, os

ONE_WAY = {('Ashburn','Frankfurt'):47, ('Ashburn','Osasco'):55, ('Ashburn','Tokyo'):75, ('Ashburn','Singapore'):104,
           ('Frankfurt','Singapore'):80, ('Frankfurt','Osasco'):100, ('Frankfurt','Tokyo'):112, ('Osasco','Tokyo'):130,
           ('Osasco','Singapore'):163, ('Singapore','Tokyo'):36}
def owd(a, b): return ONE_WAY.get((a, b), ONE_WAY.get((b, a)))

d = sys.argv[1]
nodes = sys.argv[2:] or sorted(f[:-4] for f in os.listdir(d) if f.endswith('.csv'))
R = {n: [(int(t), int(x), k) for t, x, k in csv.reader(open(os.path.join(d, n + '.csv')))] for n in nodes}
p50 = {n: sorted(x for _, x, _ in R[n])[len(R[n]) // 2] for n in nodes}
B = 10
for X in nodes:
    slow = [t + x for t, x, k in R[X] if x > max(500, 3 * p50[X])]
    if len(slow) < 15: continue
    base = [t + x for t, x, k in R[X] if x < 1.2 * p50[X]]; random.seed(1); base = random.sample(base, min(len(base), 20 * len(slow)))
    row = []
    for Y in nodes:
        if Y == X: continue
        src = sorted(t + x for t, x, k in R[Y] if x > 1.8 * p50[Y])
        def h(tg):
            c = collections.Counter()
            for t1 in tg:
                i = bisect.bisect_left(src, t1 - 300)
                while i < len(src) and src[i] <= t1 + 300: c[(t1 - src[i]) // B * B] += 1; i += 1
            return c
        hs, hb = h(slow), h(base)
        lift = {k: (hs[k] / len(slow)) / ((hb[k] + 1) / len(base)) for k in range(-300, 300, B)}
        pk = max(lift, key=lambda k: lift[k])
        row.append(f"{Y[:3]}: peak {pk:+4d}..{pk+B:+d} (lift {lift[pk]:.1f}) one-way {owd(X, Y)}")
    print(f"{X:9} n={len(slow):3}  " + " | ".join(row))
