import sys, re, collections
# Aggregates profiler.Span durations (ms) per tag within a time window.
lo, hi = sys.argv[1], sys.argv[2]
d = collections.defaultdict(list); t = dur = None
for line in sys.stdin:
    s = line.strip()
    if s.startswith('startTime'): t = s.split('=')[1].strip()[:12]
    elif s.startswith('duration'):
        v, u = s.split('=')[1].split()
        dur = float(v) * {'ns':1e-6,'us':1e-3,'ms':1,'s':1000}.get(u, 1)
    elif s.startswith('tag'):
        tag = s.split('"')[1]
        tag = re.sub(r'\|tid:\d+\|.*\]', '|key]', tag)
        if lo <= t <= hi: d[tag].append(dur)
rows = []
for k, v in d.items():
    v.sort(); n = len(v)
    rows.append((sum(v), k, n, v[n//2], v[int(n*.99)], v[-1]))
rows.sort(reverse=True)
print(f"{'tag':45} {'n':>8} {'sum_s':>9} {'p50ms':>8} {'p99ms':>8} {'maxms':>9}")
for s_, k, n, p50, p99, mx in rows[:25]:
    print(f"{k[:45]:45} {n:8} {s_/1000:9.1f} {p50:8.2f} {p99:8.1f} {mx:9.1f}")
