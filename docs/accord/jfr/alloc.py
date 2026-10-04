import sys,re,collections
# Aggregates async-profiler allocation samples (`jfr print --events jdk.ObjectAllocationInNewTLAB`, each weighted by its TLAB size) in a HH:MM:SS window.
lo,hi=sys.argv[1],sys.argv[2]
def sz(s):
    v,u=s.split()[:2]; return float(v)*{'bytes':1,'kB':1e3,'MB':1e6,'GB':1e9}[u]
by_cls=collections.Counter(); by_site=collections.Counter(); by_path=collections.Counter(); by_thr=collections.Counter(); tot=0
t=cls=th=None; w=0; fr=[]; inst=False
SKIP=re.compile(r'^(java\.|jdk\.|sun\.|io\.netty\.|org\.agrona|com\.google|net\.jpountz|org\.apache\.cassandra\.utils\.(btree|concurrent|memory|ByteBufferUtil|vint|Throwables)|accord\.utils\.(SortedArrays|ArrayBuffers|btree|Invariants))')
CATS=[('cfk load/inflate (deserialize CFK)', r'CommandsForKeyAdapter\.(load|inflate)|CommandsForKeyAccessor\.(load|unsafeLoad)|CommandsForKeySerializer.*(fromBytes|deserial)'),
      ('cfk persist/serialize', r'Serialize\.toBytes|makeUpdate|CommandsForKey.*(toBytes|serialize)'),
      ('journal write', r'AccordJournal\.saveCommand|Journal\.asyncWrite|CommandChangeWriter'),
      ('journal read/command load', r'AccordJournal\.(load|read)|IOTaskLoad|loadCommand'),
      ('deps calc', r'visitForKey|DepsCalculator|calculate'),
      ('cfk update', r'updateCommandsForKey|updateManagedCommandsForKey|CommandsForKey\.(update|insert|with)'),
      ('data read', r'TxnNamedRead|TxnRead|TxnData'),
      ('data write', r'TxnWrite|Mutation\.apply'),
      ('messaging in', r'InboundMessageHandler|FrameDecoder|deserialize'),
      ('messaging out', r'OutboundConnection|serialize'),
      ('CQL/coordinator', r'transport\.|cql3|TransactionStatement'),
      ('compaction/flush', r'ompaction|lush'),
      ('accord other', r'^accord\.'),
      ('executor', r'execution\.')]
CATS=[(c,re.compile(r)) for c,r in CATS]
by_cat=collections.Counter()
def flush():
    global tot
    if t is None or not(lo<=t<=hi): return
    tot+=w; by_cls[cls]+=w; by_thr[re.sub(r'\d+','N',th or '')]+=w
    app=[f for f in fr if not SKIP.search(f)]
    by_site[app[0] if app else (fr[0] if fr else '?')]+=w
    by_path[' < '.join(x.split('(')[0].split('.')[-2]+'.'+x.split('(')[0].split('.')[-1] for x in app[:4])]+=w
    for c,r in CATS:
        if any(r.search(f) for f in fr): by_cat[c]+=w; return
    by_cat['other']+=w
for line in sys.stdin:
    s=line.strip()
    if s.startswith('jdk.ObjectAllocation'): flush(); t=cls=th=None; w=0; fr=[]
    elif s.startswith('startTime'): t=s.split('=')[1].strip()[:12]
    elif s.startswith('objectClass'): cls=s.split('=')[1].split('(')[0].strip()
    elif s.startswith('tlabSize'): w=sz(s.split('=')[1].strip())
    elif s.startswith('eventThread'): th=s.split('"')[1]
    elif s.startswith('stackTrace'): inst=True
    elif s==']': inst=False
    elif inst and s: fr.append(re.sub(r'\s+line:.*','',s))
flush()
print(f'total sampled {tot/1e9:.1f} GB')
for name,c,n in [('category',by_cat,20),('thread',by_thr,8),('class',by_cls,20),('first app frame',by_site,30),('app path',by_path,30)]:
    print('\n== by',name)
    for k,v in c.most_common(n): print(f'{v/tot:6.1%} {k[:220]}')
