#!/usr/bin/env bash

# Report host properties that decide whether a benchmark run is trustworthy.
#
# The suite assumes it has the machine to itself and that container-to-container
# traffic is shaped only by tc.  Neither holds automatically: a Kubernetes node
# loads br_netfilter, which pushes bridged traffic through netfilter and
# conntrack, and co-tenant workloads compete for CPU and disk.  Those
# differences are invisible in the results and have to be checked separately.
#
# Usage: ./env_check.sh          (run on every machine you compare)

DIR=$(dirname "${BASH_SOURCE[0]}")
source ${DIR}/utils.sh 2>/dev/null

warn=0
say()  { printf '  %-34s %s\n' "$1" "$2"; }
flag() { printf '  %-34s %s   <-- %s\n' "$1" "$2" "$3"; warn=$((warn + 1)); }

echo "host"
say "kernel"        "$(uname -r)"
say "cpus / memory" "$(nproc) cores, $(awk '/MemTotal/{printf "%.0f GB", $2/1048576}' /proc/meminfo)"
say "cgroup"        "$(stat -fc %T /sys/fs/cgroup 2>/dev/null)"

echo "bridged traffic"
if lsmod 2>/dev/null | grep -q '^br_netfilter'; then
    v=$(sysctl -n net.bridge.bridge-nf-call-iptables 2>/dev/null)
    if [ "${v}" = "1" ]; then
        flag "br_netfilter" "loaded, call-iptables=1" "container traffic traverses netfilter+conntrack"
    else
        say "br_netfilter" "loaded, call-iptables=${v:-?}"
    fi
else
    say "br_netfilter" "not loaded (bridged traffic bypasses netfilter)"
fi

cc=$(cat /proc/sys/net/netfilter/nf_conntrack_count 2>/dev/null)
cm=$(cat /proc/sys/net/netfilter/nf_conntrack_max 2>/dev/null)
if [ -n "${cc}" ] && [ -n "${cm}" ]; then
    pct=$(awk -v c="$cc" -v m="$cm" 'BEGIN{printf "%.0f", 100*c/m}')
    if [ "${pct}" -ge 80 ]; then
        flag "conntrack" "${cc}/${cm} (${pct}%)" "table nearly full; packets get dropped"
    else
        say "conntrack" "${cc}/${cm} (${pct}%)"
    fi
    if [ -r /proc/net/stat/nf_conntrack ]; then
        d=$(awk 'NR>1 {drop += strtonum("0x" $8); ins += strtonum("0x" $10)} END {print drop+0 "," ins+0}' \
              /proc/net/stat/nf_conntrack 2>/dev/null)
        [ "${d}" = "0,0" ] && say "conntrack drop,insert_failed" "${d}" \
                           || flag "conntrack drop,insert_failed" "${d}" "netfilter is discarding packets"
    fi
fi

echo "co-tenants"
for svc in k3s k3s-agent kubelet docker; do
    if systemctl is-active --quiet "${svc}" 2>/dev/null; then
        case "${svc}" in
            docker) say "${svc}" "active" ;;
            *)      flag "${svc}" "active" "shares CPU, disk and netfilter with the benchmark" ;;
        esac
    fi
done
if command -v docker >/dev/null 2>&1; then
    total=$(docker ps -q 2>/dev/null | wc -l)
    say "running containers" "${total}"
fi
used=$(awk '/SwapTotal/{t=$2} /SwapFree/{f=$2} END{printf "%.1f", (t-f)/1048576}' /proc/meminfo)
awk -v u="$used" 'BEGIN{exit !(u > 0.1)}' && flag "swap in use" "${used} GB" "stalls land in the latency tail" \
                                          || say "swap in use" "${used} GB"

echo
if [ ${warn} -eq 0 ]; then
    echo "  no confounds detected"
else
    echo "  ${warn} condition(s) that can distort results -- see markers above"
fi
