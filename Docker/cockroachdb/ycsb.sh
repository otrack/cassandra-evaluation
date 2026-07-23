#!/usr/bin/env bash

COCKROACHDB_YCSB_DIR=$(dirname "${BASH_SOURCE[0]}")

cockroachdb_create_usertable() {
    local num_fields="$1"
    local replication_factor="$2"
    local num_dcs="$3"
    local workload="$4"

    if [[ -z "$num_fields" ]]; then
        error "Usage: cockroachdb_create_usertable <num_fields> <replication_factor> <num_dcs> <workload>"
        exit 1
    fi

    local first_city=$(get_location 1 ${DIR}/latencies.csv)
    local container="${first_city}1"

    # Set replication factor to num_dcs so each DC gets 1 replica
    local target_replicas=${num_dcs:-3}

    local create_table_command=""
    local zonecfg_command="ALTER TABLE usertable CONFIGURE ZONE USING num_replicas = ${target_replicas};"
    if [ "$workload" == "site.ycsb.workloads.ClosedEconomyWorkload" ]; then
        create_table_command="CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY, FIELD0 INT);"	
    else 	
        local fields_sql=""
        local i
        for (( i=0; i<num_fields; i++ )); do
            fields_sql+=", FIELD${i} TEXT"
        done
        create_table_command="CREATE TABLE IF NOT EXISTS usertable (YCSB_KEY VARCHAR(255) PRIMARY KEY${fields_sql});"
    fi

    docker exec "${container}" cockroach sql --insecure -e "${create_table_command}"
    if [ $? -ne 0 ]; then
        error "Error creating table."
        exit 1
    fi

    docker exec "${container}" cockroach sql --insecure -e "${zonecfg_command}"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists; zone config set (num_replicas=${target_replicas})."
    else
        error "Error setting zone config (num_replicas=${target_replicas})."
        exit 1
    fi

    local fix_lh
    fix_lh=$(config "cockroachdb.fix_lease_holder")
    if [ "${fix_lh}" = "true" ]; then
        cockroachdb_fix_lease_holder "${num_dcs}" true
    elif [ "${fix_lh}" = "bad" ]; then
        cockroachdb_fix_lease_holder "${num_dcs}" false
    fi

    local range_max_bytes=$(config "cockroachdb.range_max_bytes")
    if [ ${range_max_bytes} -ne 536870912 ]; then
        local shard_command="ALTER TABLE usertable CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = ${range_max_bytes};"
        docker exec "${container}" cockroach sql --insecure -e "${shard_command}"
    fi
}
