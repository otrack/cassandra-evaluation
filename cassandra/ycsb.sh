#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")

# cassandra_wait_schema_agreement <container> <timeout_s>
# Waits until every node reports the same schema version, as seen from
# <container>.  `nodetool describecluster` lists one "<version>: [nodes]" line
# per schema version in use, plus an UNREACHABLE one if some node is down.
cassandra_wait_schema_agreement() {
    local container=$1 timeout=$2
    local deadline=$((SECONDS + timeout)) versions
    while [ ${SECONDS} -lt ${deadline} ]; do
        # JVM_OPTS carries the server heap flags, which nodetool would inherit
        versions=$(dexec "${container}" env JVM_OPTS='' nodetool describecluster 2>/dev/null \
                       | sed -n '/Schema versions:/,/Stats for all nodes:/p' \
                       | grep -E '^[[:space:]]+[^[:space:]]+: \[')
        if [ "$(echo "${versions}" | grep -c .)" -eq 1 ] && ! echo "${versions}" | grep -q UNREACHABLE; then
            return 0
        fi
        sleep 2
    done
    return 1
}

# cassandra_cql <container> <timeout_s> <statement>
# Runs <statement> with cqlsh.  In batch mode cqlsh exits non-zero after any
# message on stderr, including its warning that the nodes have not agreed on
# the schema yet, which a DDL statement over a large or distant cluster can
# outlast (the driver waits only ~15s for agreement).  The statement has
# succeeded then, so wait for the agreement instead of failing.
cassandra_cql() {
    local container=$1 timeout=$2 statement=$3
    local err
    { err=$(dexec "${container}" -i cqlsh --request-timeout="${timeout}" -e "${statement}" 2>&1 1>&3); } 3>&1 && return 0
    if [ -n "${err}" ] && ! echo "${err}" | grep -v 'schema version mismatch' | grep -q .; then
        log "Nodes do not agree on the schema yet after '${statement}'; waiting up to ${timeout}s"
        if cassandra_wait_schema_agreement "${container}" "${timeout}"; then
            log "Schema agreement reached"
            return 0
        fi
        error "No schema agreement within ${timeout}s after '${statement}'"
        return 1
    fi
    echo "${err}" >&2
    return 1
}

cassandra_create_keyspace() {
    local timeout=$1
    local num_dcs=$2
    local replication_factor=$3

    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    local container="${first_city}1"

    # Build NetworkTopologyStrategy dict string with one replica per DC: with
    # several nodes per DC, each of them stores a share of the records (the
    # token ranges it owns), instead of every node storing every record
    local dc_map="'class': 'NetworkTopologyStrategy'"
    for i in $(seq 1 $num_dcs); do
        local city=$(get_location $i ${LOCATIONS_FILE})
        dc_map="${dc_map}, '${city}': 1"
    done
    
    drop_keyspace_command="DROP KEYSPACE IF EXISTS ycsb;"
    create_keyspace_command="CREATE KEYSPACE IF NOT EXISTS ycsb WITH replication = { ${dc_map} } AND durable_writes = false;"
    
    cassandra_cql "${container}" "${timeout}" "${drop_keyspace_command}"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' dropped if it existed."
    else
        error "Error dropping keyspace."
        exit 1
    fi

    cassandra_cql "${container}" "${timeout}" "${create_keyspace_command}"
    if [ $? -eq 0 ]; then
        debug "Keyspace 'ycsb' created with NetworkTopologyStrategy (1 replica per DC)."
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
    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    local container="${first_city}1"

    local create_table_command=""
    if [ "$workload_type" == "site.ycsb.workloads.ClosedEconomyWorkload" ] || \
       [ "$workload_type" == "site.ycsb.workloads.CalvinWorkload" ]; then
        # Numeric counters (the transactions update them with += 1)
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

    cassandra_cql "${container}" "${timeout}" "${create_table_command}"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists."
    else
        error "Error creating table."
        exit 1
    fi
}
