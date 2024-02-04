#!/bin/bash

EXP_DIR=$(dirname "${BASH_SOURCE[0]}")
BINDIR=${EXP_DIR}/..

source ${BINDIR}/context.sh
source ${BINDIR}/latency/latency.sh

min_duration() {
    if [ $# -ne 6 ]; then
        echo >&2 "usage: #clusters protocol #clients conflict_rate write_ratio operation"
        exit -1
    fi

    local ncluster=$1
    local protocol=$2
    local client=$3
    local conflict=$4
    local write=$5
    local op=$6

    init_clusters ${ncluster} false

    for cluster in ${CLUSTERS[@]}; do
        local input=$(find ${LOGDIR} -name "log-${protocol}-${cluster}-${client}-${conflict}-${write}-${op}*" | head -n 1)

        if [ "$input" == "" ] || [ ! -s "${input}" ]; then
            echo >&2 "missing file: ${LOGDIR}/log-${protocol}-${cluster}-${client}-${conflict}-${write}-${op}"
        else
            awk '{ s += $1 } END { print s }' ${input}
        fi
    done | sort -n | head -n 1
}

steady_state() {
    if [ $# -ne 2 ]; then
        echo >&2 "usage: file min_duration"
        exit -1
    fi

    local input=$1
    local min_duration=$2

    local start=$((${min_duration} * 1 / 2))
    local end=${min_duration}

    awk -v start=${start} -v end=${end} '{ s += $1; if(s >= start && s <= end) print $1 }' ${input}
}

avg_latency() {
    if [ $# -ne 6 ]; then
        echo >&2 "usage: #clusters protocol #clients conflict_rate write_ratio operation"
        exit -1
    fi

    local ncluster=$1
    local protocol=$2
    local client=$3
    local conflict=$4
    local write=$5
    local op=$6

    init_clusters ${ncluster} false

    local duration=$(min_duration $@)

    for cluster in ${CLUSTERS[@]}; do
        local input=$(find ${LOGDIR} -name "log-${protocol}-${cluster}-${client}-${conflict}-${write}-${op}*" | head -n 1)

        if [ "$input" == "" ] || [ ! -s "${input}" ]; then
            echo >&2 "missing file: ${LOGDIR}/log-${protocol}-${cluster}-${client}-${conflict}-${write}-${op}"
        else
            local result=$(steady_state ${input} ${duration} | py --ji -l 'print(numpy.mean(l),numpy.percentile(l,5), numpy.percentile(l,95))')
            echo "${cluster} ${result}"
        fi
    done
}

mad_latency() {
    if [ $# -ne 6 ]; then
        echo >&2 "usage: #clusters protocol #clients conflict_rate write_ratio operation"
        exit -1
    fi

    local values=$(avg_latency "$@" | awk '{ print $2 }')
    local mean=$(echo ${values} | tr ' ' '\n' | py --ji -l 'print(numpy.mean(l))')
    echo ${values} | tr ' ' '\n' | awk -v m=${mean} '{ d = m - $1; print d < 0 ? -d : d }' | py --ji -l 'print(numpy.mean(l))'
}

compute_tput_from_latency() {
    if [ $# -ne 3 ]; then
        echo >&2 "usage: #cclusters #clients latency"
        exit -1
    fi

    local cclusters=$1
    local client=$2
    local latency=$3

    # compute total number of clients
    local total_clients=$(echo ${cclusters} ${client} | awk '{ print $1 * $2 }')

    # compute ops/s
    # - if 1 op takes ${latency} ms, how many ops do we do in 1000 ms?
    local ops_per_sec=$(echo ${latency} | awk '{ print 1000 / $1 }')

    # compute final tput
    echo ${total_clients} ${ops_per_sec} | awk '{ print $1 * $2 }'
}


fast_path() {
    if [ $# -ne 6 ]; then
        echo >&2 "usage: #clusters protocol #clients conflict_rate write_ratio operation"
        exit -1
    fi

    local nclusters=$1
    local protocol=$2
    local client=$3
    local conflict=$4
    local write=$5
    local op=$6

    init_clusters ${nclusters} false

    for cluster in ${CLUSTERS[@]}; do
        local input=$(find ${LOGDIR} -name "path-${protocol}-${cluster}-${client}-${conflict}-${write}-${op}*" | head -n 1)

        if [ "$input" == "" ] || [ ! -s "${input}" ]; then
            echo >&2 "missing file: ${LOGDIR}/path-${protocol}-${cluster}-${client}-${conflict}-${write}-${op}"
        else
            # compute fast path ratio
            local fast_count=$(grep "fast_count" ${input} | grep -Eo "[0-9]+")
            local slow_count=$(grep "slow_count" ${input} | grep -Eo "[0-9]+")
            local ratio=$(echo ${fast_count} ${slow_count} | awk '{printf "%.2f\n", $1*100/($1 + $2)}')

            # get fast path, slow path and e2e latency
            local fast_latency=$(grep "fast" ${input} | grep -v "count" | grep -Eo "[0-9]+")
            local slow_latency=$(grep "slow" ${input} | grep -v "count" | grep -Eo "[0-9]+")
            local e2e_latency=$(grep "e2e" ${input} | grep -v "count" | cut -d- -f2 | grep -Eo "[0-9]+")

            echo "${cluster} ${ratio} ${fast_latency} ${slow_latency} ${e2e_latency}"
        fi
    done
}

opt_latency() {
    if [ $# -ne 3 ]; then
        echo >&2 "usage: protocol #sclusters #cclusters"
        exit -1
    fi

    local protocol=$1
    local sclusters=$2
    local cclusters=$3

    large_closest_quorum_latency ${protocol} ${sclusters} ${cclusters} |
        awk -v n=${cclusters} '{ s += $2 } END { print s / n }'
}

opt_tput() {
    if [ $# -ne 4 ]; then
        echo >&2 "usage: protocol #sclusters #cclusters #nclient"
        exit -1
    fi

    local protocol=$1
    local sclusters=$2
    local cclusters=$3
    local client=$4

    local latency=$(opt_latency ${protocol} ${sclusters} ${cclusters})
    compute_tput_from_latency ${cclusters} ${client} ${latency}
}

ycsb_lat() {
    if [ $# -ne 7 ]; then
        echo >&2 "usage: #clusters protocol batch_wait consistency #threads workload op"
        exit -1
    fi

    local nclusters=$1
    local protocol=$2
    local batch_wait=$3
    local consistency=$4
    local threads=$5
    local workload=$6
    local op=$7

    init_clusters ${nclusters} false

    for cluster in ${CLUSTERS[@]}; do
        local input=$(find ${LOGDIR} -name "ycsb-${batch_wait}-${consistency}-${protocol}-${cluster}-${threads}-${workload}-${op}" | head -n 1)

        if [ "${input}" == "" ] || [ ! -s "${input}" ]; then
            echo >&2 "missing file: ${LOGDIR}/ycsb-${batch_wait}-${consistency}-${protocol}-${cluster}-${threads}-${workload}-${op}"
        else
            cat ${input} | awk -v w=${workload} -v o=${op} -v c=${cluster} -v p=${protocol} -v n=${consistency}  '{print w","o","p","n","c","$2}'
        fi
    done
}

sclusters=3
init_clusters ${sclusters} false
init_log_dir "ycsb/${sclusters}"

workloads=("a" "b" "c" "d" "e")
protocols=("cassandra" "accord")
operations=("READ" "INSERT" "UPDATE" "SCAN")
consistencies=("ONE" "QUORUM" "SERIAL")
threads=10

echo "workload,operation,protocol,consistency,cluster,latency"
for workload in ${workloads[@]}; do

    for op in ${operations[@]}; do
    
	for protocol in ${protocols[@]}; do

	    for consistency in ${consistencies[@]}; do

		ycsb_lat 3 ${protocol} 0 ${consistency} ${threads} ${workload} ${op}
		
	    done
	    
	done
    done
done
