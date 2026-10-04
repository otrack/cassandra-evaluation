import sys, re, collections
# Sums jdk.ThreadPark durations per (thread pool, first non-JDK frame) in a window.
lo, hi = sys.argv[1], sys.argv[2]
agg = collections.Counter(); cnt = collections.Counter()
t = th = None; dur = 0; frames = []; inst = False
def flush():
    if t and lo <= t <= hi and frames:
        site = next((f for f in frames if not f.startswith(('java.', 'jdk.', 'sun.'))), frames[-1])
        k = (re.sub(r'\d+', 'N', th or '?'), site)
        agg[k] += dur; cnt[k] += 1
for line in sys.stdin:
    s = line.strip()
    if s.startswith('jdk.ThreadPark'): flush(); t = th = None; frames = []; dur = 0
    elif s.startswith('startTime'): t = s.split('=')[1].strip()[:12]
    elif s.startswith('duration'):
        v, u = s.split('=')[1].split(); dur = float(v) * {'ns':1e-9,'us':1e-6,'ms':1e-3,'s':1}.get(u, 1)
    elif s.startswith('eventThread'): th = s.split('"')[1]
    elif s.startswith('stackTrace'): inst = True
    elif s == ']': inst = False
    elif inst and s: frames.append(re.sub(r'\(.*', '', s))
flush()
for (th, site), v in agg.most_common(20):
    print(f"{v:8.1f}s n={cnt[(th, site)]:7}  {th:28} {site}")
