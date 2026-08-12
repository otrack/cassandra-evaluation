#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")

cassandra_create_keyspace() {
    local timeout=$1
    local num_dcs=$2
    local replication_factor=$3
    local nodes_per_dc=${4:-$(config nodesperdc)}
    nodes_per_dc=${nodes_per_dc:-1}

    local first_city=$(get_location 1 ${DIR}/latencies.csv)
    local container="${first_city}1"

    # Build NetworkTopologyStrategy dict string with nodes_per_dc replicas per DC
    local dc_map="'class': 'NetworkTopologyStrategy'"
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${DIR}/latencies.csv)
        dc_map="${dc_map}, '${city}': ${nodes_per_dc}"
    done
    
    drop_keyspace_command="DROP KEYSPACE IF EXISTS ycsb;"
    create_keyspace_command="CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = { ${dc_map} } AND durable_writes = false;"
    
    dexec ${container} -i cqlsh --request-timeout="$timeout" -e "$drop_keyspace_command"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' dropped if it existed."
    else
        error "Error dropping keyspace."
        exit 1
    fi

    dexec ${container} -i cqlsh --request-timeout="$timeout" -e "$create_keyspace_command"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' created with NetworkTopologyStrategy (${nodes_per_dc} per DC)."
    else
        log "Error creating keyspace."
        exit 1
    fi
}

cassandra_create_usertable() {
    local timeout=$1
    local transaction_mode=$2
    local num_dcs=$3
    local workload_type=$4
    local first_city=$(get_location 1 ${DIR}/latencies.csv)
    local container="${first_city}1"

    local create_table_command=""
    if [ "$workload_type" == "site.ycsb.workloads.ClosedEconomyWorkload" ]; then
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 INT)"
    elif [ "$workload_type" == "site.ycsb.workloads.ConflictWorkload" ]; then
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR)"
    else
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR)";
    fi
    
    if [ "$transaction_mode" == "full" ]; then
        create_table_command="${create_table_command} WITH transactional_mode = 'full'"
    fi
    create_table_command="${create_table_command};"

    dexec ${container} -i cqlsh --request-timeout="${timeout}" -e "${create_table_command}"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists."
    else
        error "Error creating table."
        exit 1
    fi
}
