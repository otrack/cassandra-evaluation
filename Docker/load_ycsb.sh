 #!/bin/bash

# Function to get the IP address of a container
get_container_ip() {
    container_name=$1
    ip_address=$(docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$container_name")
    echo "$ip_address"
}

# Function to get the IP addresses of all Cassandra nodes
get_all_cassandra_ips() {
    node_count=$1
    ips=""
    for i in $(seq 1 $node_count); do
        container_name="cassandra-node$i"
        ip=$(get_container_ip "$container_name")
        if [ -n "$ip" ]; then
            ips="$ips,$ip"
        fi
    done
    # Remove leading comma
    ips=${ips#,}
    echo "$ips"
}

# Function to get the number of Cassandra nodes
get_node_count() {
    i=1
    while true; do
        container_name="cassandra-node$i"
        ip=$(get_container_ip "$container_name") 2>/dev/null
        if [ -z "$ip" ]; then
            break # FIXME
        fi
        i=$((i + 1))
    done
    echo $((i - 1))
}

# Function to create the ycsb keyspace
create_keyspace() {
    timeout=$1
    node_count=$2
    drop_keyspace_command="DROP KEYSPACE IF EXISTS ycsb;"
    create_keyspace_command="CREATE KEYSPACE ycsb WITH replication = {'class': 'SimpleStrategy', 'replication_factor': $node_count};"
    
    # Drop the keyspace if it exists
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$drop_keyspace_command"
    if [ $? -eq 0 ]; then
        echo "Keyspace 'ycsb' dropped if it existed."
    else
        echo "Error dropping keyspace."
        exit 1
    fi
    
    # Create the keyspace
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$create_keyspace_command"
    if [ $? -eq 0 ]; then
        echo "Keyspace 'ycsb' created."
    else
        echo "Error creating keyspace."
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
        echo "Table 'usertable' created or already exists."
    else
        echo "Error creating table."
        exit 1
    fi

    # Truncate the table to empty it
    docker exec -i cassandra-node"$node_count" cqlsh --request-timeout="$timeout" -e "$truncate_table_command"
    if [ $? -eq 0 ]; then
        echo "Table 'usertable' truncated."
    else
        echo "Error truncating table."
        exit 1
    fi
}

run_ycsb() {
    action=$1
    ycsb_dir=$2
    workload=$3
    hosts=$4
    recordcount=$5
    operationcount=$6
    consistency_level=$7
    output_file=$8
    threads=$9
    retries=1
    
    echo "Starting YCSB"
    debug="JAVA_OPTS=\"-Dorg.slf4j.simpleLogger.defaultLogLevel=debug\"" # comment out to have debug on
    cmd="${debug} $ycsb_dir/bin/ycsb.sh $action cassandra-cql -p hosts=$hosts -P $ycsb_dir/$workload -p cassandra.writeconsistencylevel=$consistency_level -p cassandra.readconsistencylevel=$consistency_level -p recordcount=$recordcount -p operationcount=$operationcount -threads $threads -s"
    eval "$cmd" | tee "$output_file"
    if [ $? -eq 0 ]; then
        echo "YCSB $action completed successfully."
        return 0
    fi
    echo "YCSB $action failed after $retries attempts."
    exit 1
}


# Function to stop a container after a delay
stop_container_after_delay() {
    container_name=$1
    delay=$2
    (
        sleep "$delay"
        docker stop "$container_name"
        echo "Stopped container '$container_name' after $delay seconds."
    ) &
}

# Main script
if [ $# -ne 7 ]; then
    echo "Usage: $0 <consistency_level> <number_of_threads> <transaction_mode> <output_file> <workload> <record_count> <operation_count>"
    echo "Example: $0 ONE 1 full results.txt a"
    exit 1
fi

consistency_level=$1
nthreads=$2
transaction_mode=$3
output_file=$4
workload="workloads/workload$5"
record_count=$6
operation_count=$7

ycsb_dir="/home/otrack/Implementation/YCSB"
node_count=$(get_node_count)
# hosts=$(get_all_cassandra_ips "$node_count")
hosts=$(get_container_ip "cassandra-node$node_count")
if [ -z "$hosts" ]; then
    echo "Failed to retrieve the IP addresses."
    exit 1
fi

# Create the keyspace if it doesn't exist
create_keyspace 3600 "$node_count"

# Create the usertable if it doesn't exist
create_usertable 3600 "$transaction_mode" "$node_count"

# Load data and write performance results to the output file
run_ycsb "load" "$ycsb_dir" "$workload" "$hosts" "$record_count" "$operation_count" "$consistency_level" "$output_file" "$nthreads"

# Simulate a node crash after 2 minutes
# stop_container_after_delay "cassandra-node2" 90

