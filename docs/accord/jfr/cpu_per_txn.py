import sys, glob, re, os
# Usage: cpu_per_txn.py <cpu.csv> <dat prefix (without _DC.dat)>
# CPU of each node over the last 60s of the window where the 3 YCSB run clients are up,
# divided by the total throughput (tx/s summed over DCs).
csv, prefix = sys.argv[1], sys.argv[2]
rows=[]
for l in open(csv):
    f=l.strip().split(',')
    if "NA" in l or "OCI" in l or l.count("=")<4: continue
    t=int(f[0]); d={k:int(v) for k,v in (x.split('=') for x in f[1:4])}; y=int(f[4].split('=')[1])
    rows.append((t,d,y))
# the contiguous block with ycsb==3 that ends closest to (before) the .dat files' last write
mt=max(os.path.getmtime(f'{prefix}_{dc}.dat') for dc in ['Hanoi','Lyon','NewYork'] if os.path.exists(f'{prefix}_{dc}.dat'))
blks=[]; cur=[]
for r in rows:
    if r[2]==3: cur.append(r)
    else:
        if cur: blks.append(cur)
        cur=[]
if cur: blks.append(cur)
blk=min(blks, key=lambda b: abs(b[-1][0]-mt))
end=blk[-2]; start=next(r for r in blk if r[0]>=end[0]-60)
dt=end[0]-start[0]
tput=0; lat=[]
for dc in ['Hanoi','Lyon','NewYork']:
    fn=f'{prefix}_{dc}.dat'
    if not os.path.exists(fn): continue
    s=open(fn).read()
    m=re.search(r'\[OVERALL\], Throughput\(ops/sec\), ([\d.]+)',s); tput+=float(m.group(1))
    m=re.search(r'\[(?:CHECK_AND_INCREMENT|CALVIN|TX[^\]]*)\], AverageLatency\(us\), ([\d.]+)',s)
    lat.append(float(m.group(1))/1000 if m else float('nan'))
cores={n:(end[1][n]-start[1][n])/1e6/dt for n in end[1]}
print(f'tput={tput:.0f} tx/s  avglat/DC(ms)={[round(x) for x in lat]}  window={dt}s')
for n,c in cores.items(): print(f'  {n}: {c:5.2f} cores  {c*1000/tput:6.2f} core-ms/txn')
