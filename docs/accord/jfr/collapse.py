import sys, re, argparse, collections
# Turns `jfr print --events <jdk.ExecutionSample|profiler.WallClockSample> --stack-depth N`
# output into collapsed stacks (root;...;leaf count) for FlameGraph, rooted at the thread
# pool. CPU samples taken by the wall-clock engine are dropped from jdk.ExecutionSample.
p = argparse.ArgumentParser()
p.add_argument('lo'); p.add_argument('hi')
p.add_argument('--threads', default='.', help='regex on the thread name')
p.add_argument('--state', default=None, help='keep only wall samples in this state, e.g. RUNNABLE')
a = p.parse_args()
thr = re.compile(a.threads)
out = collections.Counter()
t = th = st = None; w = 1; frames = []; inst = False
def flush():
    if not (t and a.lo <= t <= a.hi and frames and thr.search(th or '')): return
    if 'WallClock::signalHandler' in frames[0]: return
    if a.state and (st is None or a.state not in st): return
    stack = [re.sub(r'\d+', 'N', th)] + [f for f in reversed(frames) if f != '...']
    out[';'.join(stack)] += w
for line in sys.stdin:
    s = line.strip()
    if s.endswith('{') and not s.startswith(('stackTrace',)):
        flush(); t = th = st = None; w = 1; frames = []
    elif s.startswith('startTime'): t = s.split('=')[1].strip()[:12]
    elif s.startswith('sampledThread'): th = s.split('"')[1]
    elif s.startswith('state ='): st = s.split('=')[1].strip()
    elif s.startswith('samples ='): w = int(s.split('=')[1])
    elif s.startswith('stackTrace'): inst = True
    elif s == ']': inst = False
    elif inst and s: frames.append(re.sub(r'\(.*|\s+line:.*', '', s).replace(';', ':'))
flush()
for k, v in out.items(): print(k, v)
