#!/bin/bash
# Every 5s: epoch, per-node cgroup CPU usage_usec, and whether YCSB clients are running. Stops on $1.stop
out=$1
while [ ! -f "$out.stop" ]; do
  t=$(date +%s); line="$t"
  for n in Hanoi1 Lyon1 NewYork1; do
    u=$(docker exec $n sh -c 'grep usage_usec /sys/fs/cgroup/cpu.stat | head -1 | cut -d" " -f2' 2>/dev/null)
    line="$line,$n=${u:-NA}"
  done
  y=$(docker ps --format '{{.Names}}' | grep -c '^ycsb-')
  echo "$line,ycsb=$y" >> $out
  sleep 5
done
