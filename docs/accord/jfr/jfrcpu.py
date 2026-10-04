import sys, re, collections
# Aggregates jdk.ExecutionSample text output (from `jfr print`) within a time window.
lo, hi = sys.argv[1], sys.argv[2]
pool = collections.Counter(); self_ = collections.Counter(); incl = collections.Counter()
total = 0
t = th = None; frames = []; instack = False
def norm(th):
    return re.sub(r'\d+', 'N', th)
def flush():
    global total
    if t is None or not (lo <= t <= hi) or not frames: return
    if 'WallClock::signalHandler' in frames[0]: return
    total += 1
    pool[norm(th)] += 1
    jf = [f for f in frames if not f.startswith(('lib', './', '['))] or frames
    self_[jf[0]] += 1
    for f in set(jf):
        if 'accord' in f or 'cassandra' in f: incl[f] += 1
for line in sys.stdin:
    s = line.strip()
    if s.startswith('jdk.ExecutionSample'):
        flush(); t = th = None; frames = []
    elif s.startswith('startTime'):
        t = s.split('=')[1].strip()[:12]
    elif s.startswith('sampledThread'):
        th = s.split('"')[1]
    elif s.startswith('stackTrace'):
        instack = True
    elif s == ']':
        instack = False
    elif instack and s:
        frames.append(re.sub(r'\(.*', '', s.replace('...', '')))
flush()
print("on-CPU samples:", total)
print("\n== by thread pool"); [print(f"{c/total:6.1%}  {k}") for k, c in pool.most_common(15)]
print("\n== self (top frame)"); [print(f"{c/total:6.1%}  {k}") for k, c in self_.most_common(30)]
print("\n== inclusive accord/cassandra frames"); [print(f"{c/total:6.1%}  {k}") for k, c in incl.most_common(60)]
