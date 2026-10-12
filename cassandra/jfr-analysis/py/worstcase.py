#!/usr/bin/env python3
"""Per-site worst-case Accord latency bounds, as in AccordYcsbLoadTest.computeWorstLatencies, for a one-way delay matrix.

  worstcase.py            # the AWS 5-region matrix below (one-way ms, measured from stall propagation, +/- a few ms)
  qs[i]  = median of row i (one-way delay to a simple majority, incl. self)
  worst1 = max_j  qs[i] + 3*qs[j] + L[i][j]
  worst2 = max_{j != i, qs[j] <= 2*qs[i]}  qs[i] + 4*qs[j] + L[i][j]
These model a single conflicting coordinator; chains of dependencies across regions can exceed them.
"""
NAMES = ['Ashburn', 'Frankfurt', 'Osasco', 'Singapore', 'Tokyo']
OW = {('Ashburn','Frankfurt'):47, ('Ashburn','Osasco'):55, ('Ashburn','Tokyo'):75, ('Ashburn','Singapore'):104,
      ('Frankfurt','Singapore'):80, ('Frankfurt','Osasco'):100, ('Frankfurt','Tokyo'):112, ('Osasco','Tokyo'):130,
      ('Osasco','Singapore'):163, ('Singapore','Tokyo'):36}
def ow(a, b): return 0 if a == b else OW.get((a, b), OW.get((b, a)))
L = [[ow(a, b) for b in NAMES] for a in NAMES]
n = len(L); qs = [sorted(r)[n // 2] for r in L]
print(f"{'site':10} {'quorumRTT':>9} {'worst1':>7} {'worst2':>7}")
for i in range(n):
    w1 = max(qs[i] + 3 * qs[j] + L[i][j] for j in range(n))
    w2 = max([qs[i] + 4 * qs[j] + L[i][j] for j in range(n) if j != i and qs[j] <= 2 * qs[i]] or [0])
    print(f"{NAMES[i]:10} {2*qs[i]:9} {w1:7} {w2:7}")
