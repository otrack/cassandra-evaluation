#!/usr/bin/env bash

COCKROACHDB_YCSB_DIR=$(dirname "${BASH_SOURCE[0]}")

# Function to create the usertable in CockroachDB
cockroachdb_create_usertable() {
    local container=$(config "node_name")1
    local create_table_command="CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 TEXT, FIELD1 TEXT, FIELD2 TEXT, FIELD3 TEXT, FIELD4 TEXT, FIELD5 TEXT, FIELD6 TEXT, FIELD7 TEXT, FIELD8 TEXT, FIELD9 TEXT);"

    docker exec ${container} cockroach sql --insecure -e "$create_table_command"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists."
    else
        error "Error creating table."
        exit 1
    fi
}
