#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

## Cassandra

cassandra_create_keyspace() {
    local timeout=$1
    local node_count=$2
    drop_keyspace_command="DROP KEYSPACE IF EXISTS ycsb;"
    create_keyspace_command="CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $node_count};"
    
    # Drop the keyspace if it exists
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$drop_keyspace_command"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' dropped if it existed."
    else
        error "Error dropping keyspace."
        exit 1
    fi
    
    # Create the keyspace
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$create_keyspace_command"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' created."
    else
        log "Error creating keyspace."
        exit 1
    fi
}

# Function to create the usertable
cassandra_create_usertable() {
    local timeout=$1
    local transaction_mode=$2
    local node_count=$3
    truncate_table_command="TRUNCATE ycsb.usertable;"
    if [ "$transaction_mode" == "full" ]; then
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR) WITH transactional_mode = 'full';"
    else
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR);"
    fi

    # Create the table if it does not exist
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$create_table_command"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists."
    else
        error "Error creating table."
        exit 1
    fi

    # Truncate the table to empty it
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$truncate_table_command"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' truncated."
    else
        error "Error truncating table."
        exit 1
    fi
}

cassandra_run_ycsb() {
    if [ $# -lt 10 ]; then
	echo "Usage: $0 <action> <workload_type> <workload> <hosts> <port> <recordcount> <operation_count> <protocol> <output_file> <threads>"
	echo "Example: $0 load site.ycsb.CoreWorkload a 127.0.0.1,127.0.0.2 8080 1 1 QUORUM results.txt 100"
	exit 1
    fi

    local action=$1
    local workload_type=$2
    local workload=$3
    local hosts=$4
    local port=$5
    local recordcount=$6
    local operationcount=$7
    local protocol=$8
    local consistency_level="ONE"
    if [ "$protocol" == "accord" ] || [ "$protocol" == "paxos" ];
    then
	consistency_level="SERIAL"
    elif [ "$protocol" == "quorum" ];
    then
	consistency_level="QUORUM"
    fi
    local output_file=$9
    local threads=${10}

    # capture any extra arguments (11th onward) and prepare a safely quoted string
    shift 10
    local extra_opts=( "$@" )
    local extra_opts_str=""
    if [ ${#extra_opts[@]} -gt 0 ]; then
      for o in "${extra_opts[@]}"; do
        # printf %q produces a shell-escaped representation; safe to append to the command string
        extra_opts_str+=" $(printf '%q' "$o")"
      done
    fi
    
    ycsb_dir=$(config ycsb_dir)
    
    hdr_file=output_file.hdr

    if [ "$action" == "load" ];
    then

	# Determine transaction mode
	local transaction_mode="bruh"
	if [ "$protocol" == "accord" ]; then 
	    transaction_mode="full"
	fi

	# Create the keyspace if it doesn't exist
	cassandra_create_keyspace 3600 "$node_count"

	# Create the usertable if it doesn't exist
	cassandra_create_usertable 3600 "$transaction_mode" "$node_count"
    fi

    #debug="JAVA_OPTS=\"-Dorg.slf4j.simpleLogger.defaultLogLevel=debug\"" # comment out to have debug on
    cmd="${debug} $ycsb_dir/bin/ycsb.sh $action cassandra-cql \
    -p workload=$workload_type \
    -P ${ycsb_dir}/workloads/workload${workload} \
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
    -threads $nthreads -s ${extra_opts_str}"

    eval "$cmd" | tee "$output_file"
    if [ $? -eq 0 ]; then
        log "YCSB $action completed successfully."
    else
        log "Error running YCSB $action."
        exit 1
    fi
}
