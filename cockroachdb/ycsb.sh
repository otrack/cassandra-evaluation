#!/usr/bin/env bash

COCKROACHDB_YCSB_DIR=$(dirname "${BASH_SOURCE[0]}")

cockroachdb_create_usertable() {
    local num_fields="$1"
    local replication_factor="$2"
    local num_dcs="$3"
    local workload="$4"
    local record_count="$5"

    if [[ -z "$num_fields" ]]; then
        error "Usage: cockroachdb_create_usertable <num_fields> <replication_factor> <num_dcs> <workload> [record_count]"
        exit 1
    fi

    local first_city=$(get_location 1 ${LOCATIONS_FILE})
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

    dexec "${container}" cockroach sql --insecure -e "${create_table_command}"
    if [ $? -ne 0 ]; then
        error "Error creating table."
        exit 1
    fi

    dexec "${container}" cockroach sql --insecure -e "${zonecfg_command}"
    if [ $? -eq 0 ]; then
        debug "Table 'usertable' created or already exists; zone config set (num_replicas=${target_replicas})."
    else
        error "Error setting zone config (num_replicas=${target_replicas})."
        exit 1
    fi

    local fix_lh
    fix_lh=$(config "cockroachdb.fix_lease_holder")
    COCKROACHDB_LEASE_CITY=""
    if [ "${fix_lh}" = "true" ]; then
        cockroachdb_fix_lease_holder "${num_dcs}" true
    elif [ "${fix_lh}" = "bad" ]; then
        cockroachdb_fix_lease_holder "${num_dcs}" false
    fi

    local range_max_bytes=$(config "cockroachdb.range_max_bytes")
    if [ ${range_max_bytes} -ne 536870912 ]; then
        local shard_command="ALTER TABLE usertable CONFIGURE ZONE USING range_min_bytes = 0, range_max_bytes = ${range_max_bytes};"
        dexec "${container}" cockroach sql --insecure -e "${shard_command}"
    fi

    local partitions
    partitions=$(config "cockroachdb.partitions")
    if [ -n "${partitions}" ] && [ "${partitions}" -gt 1 ]; then
        cockroachdb_partition_usertable "${partitions}" "${record_count}" "${COCKROACHDB_LEASE_CITY}"
    fi
}

# cockroachdb_partition_usertable <partitions> <record_count> [lease_city]
#
# Splits usertable into <partitions> ranges of contiguous keys, as the
# "range" partitioner of the Calvin micro-benchmark does (zero-padded keys,
# partition p holding the key numbers [p*R/N, (p+1)*R/N)).  When the lease
# holders are pinned to <lease_city>, range p is a table partition whose
# replica in that region, and lease, go to the node of zone p+1 there, so that
# each node of the region leads one partition; otherwise the ranges are
# scattered over the cluster.
cockroachdb_partition_usertable() {
    local partitions=$1
    local record_count=$2
    local lease_city=$3
    if [ -z "${record_count}" ]; then
        error "cockroachdb_partition_usertable: the number of records is required"
        exit 1
    fi

    local first_city=$(get_location 1 ${LOCATIONS_FILE})
    local container="${first_city}1"
    # The keys are padded to the digits of the largest key number
    local largest=$(( record_count - 1 ))
    local width=${#largest}

    local stmt="ALTER TABLE usertable PARTITION BY RANGE (ycsb_key) ("
    local p lower upper
    for p in $(seq 0 $(( partitions - 1 ))); do
        if [ "${p}" -eq 0 ]; then
            lower="MINVALUE"
        else
            lower="'user$(printf "%0${width}d" $(( p * record_count / partitions )))'"
        fi
        if [ "${p}" -eq $(( partitions - 1 )) ]; then
            upper="MAXVALUE"
        else
            upper="'user$(printf "%0${width}d" $(( (p + 1) * record_count / partitions )))'"
        fi
        [ "${p}" -gt 0 ] && stmt+=", "
        stmt+="PARTITION p${p} VALUES FROM (${lower}) TO (${upper})"
    done
    stmt+=");"

    if [ -n "${lease_city}" ]; then
        for p in $(seq 0 $(( partitions - 1 ))); do
            local zone=$(( p + 1 ))
            stmt+=" ALTER PARTITION p${p} OF TABLE usertable CONFIGURE ZONE USING num_replicas = COPY FROM PARENT,"
            stmt+=" constraints = '{\"+region=${lease_city},+zone=${zone}\": 1}',"
            stmt+=" lease_preferences = '[[\"+region=${lease_city}\", \"+zone=${zone}\"]]';"
        done
    else
        stmt+=" ALTER TABLE usertable SCATTER;"
    fi

    log "Partitioning usertable into ${partitions} ranges${lease_city:+ led by the nodes of ${lease_city}}..."
    if ! dexec "${container}" cockroach sql --insecure -e "${stmt}"; then
        error "Error partitioning usertable: ${stmt}"
        exit 1
    fi
}
