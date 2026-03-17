#!/usr/bin/env bash

COCKROACHDB_YCSB_DIR=$(dirname "${BASH_SOURCE[0]}")

# Function to create the usertable in CockroachDB
# Usage: cockroachdb_create_usertable <num_fields> <replication_factor>
cockroachdb_create_usertable() {
    local num_fields="$1"
    local replication_factor="$2"
    local node_count="$3"
    local workload="$4"

    if [[ -z "$num_fields" || -z "$replication_factor" ]]; then
        error "Usage: cockroachdb_create_usertable <num_fields> <replication_factor>"
        exit 1
    fi
    if ! [[ "$num_fields" =~ ^[0-9]+$ ]] || (( num_fields < 0 )); then
        error "num_fields must be a non-negative integer (got: $num_fields)"
        exit 1
    fi
    if ! [[ "$replication_factor" =~ ^[0-9]+$ ]] || (( replication_factor < 1 )); then
        error "replication_factor must be a positive integer (got: $replication_factor)"
        exit 1
    fi

    local container
    container="$(config "node_name")1"

    local create_table_command=""
    local zonecfg_command="ALTER TABLE usertable CONFIGURE ZONE USING num_replicas = ${replication_factor};"
    if [ "$workload" == "site.ycsb.workloads.ClosedEconomyWorkload" ]; then
	create_table_command="CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 INT);"	
    else 	
	# Build the FIELD0..FIELD{N-1} column list dynamically.
	local fields_sql=""
	local i
	for (( i=0; i<num_fields; i++ )); do
            fields_sql+=", FIELD${i} TEXT"
	done
	create_table_command="CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY${fields_sql});"
    fi

    # Create table, then set per-table replication factor via zone config.
    docker exec "${container}" cockroach sql --insecure -e "${create_table_command}"
    if [ $? -ne 0 ]; then
        error "Error creating table."
        exit 1
    fi

    docker exec "${container}" cockroach sql --insecure -e "${zonecfg_command}"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists; zone config set (num_replicas=${replication_factor})."
    else
        error "Error setting zone config (num_replicas=${replication_factor})."
        exit 1
    fi

    # Use a single shard?
    if [ "$replication_factor" == "$node_count" ];
    then
	local shard_command="ALTER TABLE usertable CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = 1073741824;"
	docker exec "${container}" cockroach sql --insecure -e "${shard_command}"
    fi
}
