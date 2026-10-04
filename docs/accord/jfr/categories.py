import sys, re, collections
# Categorises on-CPU samples (`jfr print --events jdk.ExecutionSample`) in a HH:MM:SS.mmm window; the gc rule matches ZGC only.
lo, hi = sys.argv[1], sys.argv[2]
# A sample goes to the first rule, in list order, that matches any of its frames.
RULES = [
 ('gc', r'^libjvm\.so\.Z|ZWorker'),
 ('cfk load (system table read)', r'CommandsForKeyAccessor\.(load|unsafeLoad)'),
 ('command load (journal read)', r'IOTaskLoad|AccordJournal\.load|loadCommand|Journal\.read'),
 ('data read (TxnNamedRead)', r'TxnNamedRead|TxnRead'),
 ('data write (TxnWrite/apply mutation)', r'TxnWrite|Mutation\.apply|Keyspace\.apply'),
 ('journal write', r'AccordJournal\.saveCommand|Journal\.asyncWrite|CommandChangeWriter'),
 ('cfk persist (system table write)', r'AccordKeyspace.*(Mutation|save|update)|CommandsForKeyAccessor\.(save|write)'),
 ('deps calc', r'visitForKey|DepsCalculator|calculateDeps|mapReduceActive|CommandsForKey\.(mapReduce|visit)'),
 ('cfk update', r'updateCommandsForKey|updateManagedCommandsForKey|CommandsForKey\.update'),
 ('cache evict/shrink', r'AccordCache\.(shrinkOrEvict|tryShrinkOrEvict|evict)'),
 ('metrics', r'metrics\.|Histogram\.update|Timer\.update'),
 ('messaging in (decode)', r'FrameDecoder|InboundMessageHandler|deserialize'),
 ('messaging out (encode)', r'OutboundConnection|FrameEncoder|serialize'),
 ('CQL / coordinator', r'transport\.|TransactionStatement|QueryProcessor|cql3'),
 ('compaction/flush', r'Compaction|Flush|flush'),
 ('jit', r'CompilerThre'),
 ('accord protocol logic', r'^accord\.'),
 ('executor/lock/park', r'Unsafe\.(park|unpark)|AccordExecutor|AbstractLockLoop|LockSupport'),
]
RULES=[(c,re.compile(r)) for c,r in RULES]
cnt=collections.Counter(); tot=0
t=th=None; frames=[]; inst=False
def flush():
    global tot
    if t is None or not (lo<=t<=hi) or not frames: return
    if 'WallClock::signalHandler' in frames[0]: return
    tot+=1
    fs=[th]+frames
    for c,r in RULES:
        if any(r.search(f) for f in fs): cnt[c]+=1; return
    cnt['other']+=1
for line in sys.stdin:
    s=line.strip()
    if s.startswith('jdk.ExecutionSample'): flush(); t=th=None; frames=[]
    elif s.startswith('startTime'): t=s.split('=')[1].strip()[:12]
    elif s.startswith('sampledThread'): th=s.split('"')[1]
    elif s.startswith('stackTrace'): inst=True
    elif s==']': inst=False
    elif inst and s: frames.append(re.sub(r'\(.*','',s.replace('...','')))
flush()
print('samples',tot)
for c,n in cnt.most_common(): print(f'{n/tot:6.1%}  {c}')
