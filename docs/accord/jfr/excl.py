import sys, re, collections
# For on-CPU samples whose stack runs under the executor lock (Exclusive*), counts inclusive frames.
lo, hi = sys.argv[1], sys.argv[2]
incl = collections.Counter(); n = 0; all_ = 0
t = None; frames = []; inst = False
def flush():
    global n, all_
    if not (t and lo <= t <= hi and frames) or 'WallClock::signalHandler' in frames[0]: return
    all_ += 1
    if not any('Exclusive' in f for f in frames): return
    n += 1
    for f in set(frames):
        if f.startswith(('accord.', 'org.apache.cassandra.')): incl[f] += 1
for line in sys.stdin:
    s = line.strip()
    if s.startswith('jdk.ExecutionSample'): flush(); t = None; frames = []
    elif s.startswith('startTime'): t = s.split('=')[1].strip()[:12]
    elif s.startswith('stackTrace'): inst = True
    elif s == ']': inst = False
    elif inst and s: frames.append(re.sub(r'\(.*', '', s))
flush()
print(f"samples under lock: {n} of {all_} on-CPU ({n/all_:.1%})")
skip = ('Loops', 'LockLoop', 'CassandraThread', 'runNoExcept', 'Lambda')
for f, c in incl.most_common(70):
    if not any(x in f for x in skip): print(f"{c/n:6.1%}  {f}")
