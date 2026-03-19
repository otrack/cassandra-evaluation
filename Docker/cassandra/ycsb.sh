#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")

cassandra_create_keyspace() {
    local timeout=$1
    local node_count=$2
    local replication_factor=$3
    local container=$(config "node_name")${node_count}

    if ! [[ "$replication_factor" =~ ^[0-9]+$ ]] || (( replication_factor < 1 )); then
        error "replication_factor must be a positive integer (got: $replication_factor)"
        exit 1
    fi
    
    drop_keyspace_command="DROP KEYSPACE IF EXISTS ycsb;"
    create_keyspace_command="CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': ${replication_factor}} AND durable_writes = false;"
    
    # Drop the keyspace if it exists
    docker exec -i ${container} cqlsh --request-timeout="$timeout" -e "$drop_keyspace_command"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' dropped if it existed."
    else
        error "Error dropping keyspace."
        exit 1
    fi

    
    # Create the keyspace
    docker exec -i ${container} cqlsh --request-timeout="$timeout" -e "$create_keyspace_command"
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
    local workload_type=$4
    local container=$(config "node_name")${node_count}	

    local create_table_command=""
    if [ "$workload_type" == "site.ycsb.workloads.ClosedEconomyWorkload" ]; then
	create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 INT)"
    else # site.ycsb.workloads.{CoreWorkload, SwapWorkload}"
	create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR)";
    fi
    
    if [ "$transaction_mode" == "full" ]; then
        create_table_command="${create_table_command} WITH transactional_mode = 'full'"
    fi
    create_table_command="${create_table_command};"

    # Create the table if it does not exist
    docker exec -i ${container} cqlsh --request-timeout="${timeout}" -e "${create_table_command}"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists."
    else
        error "Error creating table."
        exit 1
    fi

    # FIXME skipped FTM to avoids bugs
    #
    # Truncate the table to empty it
    # truncate_table_command="TRUNCATE ycsb.usertable;"
    # docker exec -i ${container} cqlsh --request-timeout="$timeout" -e "$truncate_table_command"
    # if [ $? -eq 0 ]; then
    #     debug "Table 'usertable' truncated."
    # else
    #     error "Error truncating table."
    #     exit 1
    # fi
}
