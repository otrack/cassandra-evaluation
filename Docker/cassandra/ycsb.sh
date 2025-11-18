#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")

cassandra_create_keyspace() {
    local timeout=$1
    local node_count=$2
    local container=$(config "node_name")${node_count}
    drop_keyspace_command="DROP KEYSPACE IF EXISTS ycsb;"
    create_keyspace_command="CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $node_count};"
    
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
    local container=$(config "node_name")${node_count}	
    truncate_table_command="TRUNCATE ycsb.usertable;"
    if [ "$transaction_mode" == "full" ]; then
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR) WITH transactional_mode = 'full';"
    else
        create_table_command="CREATE TABLE IF NOT EXISTS ycsb.usertable (y_id VARCHAR PRIMARY KEY, field0 VARCHAR, field1 VARCHAR, field2 VARCHAR, field3 VARCHAR, field4 VARCHAR, field5 VARCHAR, field6 VARCHAR, field7 VARCHAR, field8 VARCHAR, field9 VARCHAR);"
    fi

    # Create the table if it does not exist
    docker exec -i ${container} cqlsh --request-timeout="$timeout" -e "$create_table_command"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists."
    else
        error "Error creating table."
        exit 1
    fi

    # Truncate the table to empty it
    docker exec -i ${container} cqlsh --request-timeout="$timeout" -e "$truncate_table_command"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' truncated."
    else
        error "Error truncating table."
        exit 1
    fi
}
