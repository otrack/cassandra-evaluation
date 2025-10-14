#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

# Function to run YCSB
run_ycsb() {
    action=$1
    ycsb_dir=$2
    workload=$3
    hosts=$4
    port=$5
    recordcount=$6
    operationcount=$7
    protocol=$8
    if [ "$protocol" == "accord" ] || [ "$protocol" == "paxos" ];
    then
	consistency_level="SERIAL"
    elif [ "$protocol" == "quorum" ];
    then
	consistency_level="QUORUM"
    else
	consistency_level="ONE"
    fi
    output_file=$9
    threads=${10}

    debug ${nthreads}
    
    hdr_file=output_file.hdr
    
    #debug="JAVA_OPTS=\"-Dorg.slf4j.simpleLogger.defaultLogLevel=debug\"" # comment out to have debug on
    cmd="${debug} $ycsb_dir/bin/ycsb.sh $action cassandra-cql \
    -P $ycsb_dir/$workload \
    -p hosts=$hosts \
    -p port=$port \
    -p cassandra.writeconsistencylevel=$consistency_level \
    -p cassandra.readconsistencylevel=$consistency_level \
    -p recordcount=$recordcount \
    -p operationcount=$operationcount \
    -p measurementtype=hdrhistogram \
    -p hdrhistogram.fileoutput=false \
    -p hdrhistogram.output.path=${DIR}/${hdr_file} \
    -p hdrhistogram.percentiles=$(seq -s, 1 100) \
    -threads $nthreads -s"

    eval "$cmd" | tee "$output_file"
    if [ $? -eq 0 ]; then
        log "YCSB $action completed successfully."
    else
        log "Error running YCSB $action."
        exit 1
    fi
}

ycsb_dir=$(config ycsb_dir)
node_count=$(get_node_count)
hosts=$(get_all_cassandra_ips "${node_count}")
port=9042

debug "node_count:${node_count}"
debug "hosts:${hosts}"
debug "port:${port}"

if [ -z "$hosts" ]; then
    echo "Failed to retrieve the IP addresses."
    exit 1
fi
