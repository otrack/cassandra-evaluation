#!/usr/bin/env bash

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/functions_ycsb.sh

# Function to create the ycsb keyspace
create_keyspace() {
    timeout=$1
    node_count=$2
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
create_usertable() {
    timeout=$1
    transaction_mode=$2
    node_count=$3
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

# Main script
if [ $# -ne 7 ]; then
    echo "Usage: $0 <protocol> <number_of_threads> <output_file> <workload> <record_count> <operation_count>"
    echo "Example: one 1 results.txt a 1 1"
    echo "Was: $@ ($#)"
    exit 1
fi

protocol=$1
nthreads=$2
# Determine transaction mode
if [ "$protocol" == "accord" ]; then 
    transaction_mode="full"
else
    transaction_mode="bruh"
fi
output_file=$3
workload="workloads/workload$4"
record_count=$5
operation_count=$6

# Create the keyspace if it doesn't exist
create_keyspace 3600 "$node_count"

# Create the usertable if it doesn't exist
create_usertable 3600 "$transaction_mode" "$node_count"

# Load data and write performance results to the output file
run_ycsb "load" "$ycsb_dir" "$workload" "$hosts" "$port" "$record_count" "$operation_count" "$protocol" "$output_file" "$nthreads"

# Simulate a node crash after 2 minutes
# stop_container_after_delay "cassandra-node2" 90

