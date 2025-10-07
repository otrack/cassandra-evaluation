#!/bin/bash

# Usage: ./parse_ycsb_files_to_csv.sh <file1> <file2> ... > output.csv
# Output columns: consistency_level,number_nodes,workload,phase,operation_type,number_clients,average_latency_ms

echo "consistency_level,number_nodes,workload,phase,operation_type,number_clients,average_latency_ms"

for file in "$@"; do
    filename=$(basename "$file")

    # Parse: <consistency_level>_<number_nodes>_[load|run]_<workload>.dat
    if [[ "$filename" =~ ^([^_]+)_([0-9]+)_(load|run)_([^\.]+)\.dat$ ]]; then
        consistency_level="${BASH_REMATCH[1]}"
        number_nodes="${BASH_REMATCH[2]}"
        phase="${BASH_REMATCH[3]}"
        workload="${BASH_REMATCH[4]}"
    else
        consistency_level="unknown"
        number_nodes="unknown"
        phase="unknown"
        workload="unknown"
    fi

    # Extract number_clients (threads) from the file
    number_clients=$(grep -E 'Threads:|threadcount' "$file" | head -1 | grep -oE '[0-9]+')
    if [ -z "$number_clients" ]; then
        number_clients=$(grep -oE '\-threads[ =][0-9]+' "$file" | head -1 | grep -oE '[0-9]+')
    fi
    if [ -z "$number_clients" ]; then
        number_clients="unknown"
    fi

    # Parse operation lines: [OPERATION], AverageLatency(us), 123.45
    grep -E '^\[.*\], AverageLatency\(us\),' "$file" | while IFS=, read -r op latency value; do
        operation_type=$(echo "$op" | sed -E 's/^\[\s*(.*)\s*\]$/\1/' | tr '[:upper:]' '[:lower:]')
        average_latency_us=$(echo "$value" | xargs)
        # Only output for specific operation types
        case "$operation_type" in
            read|insert|update|scan|readmodifywrite)
                # Convert microseconds to milliseconds and round
                average_latency_ms=$(awk "BEGIN { print int( ($average_latency_us/1000) + 0.5 ) }")
                echo "$consistency_level,$number_nodes,$workload,$phase,$operation_type,$number_clients,$average_latency_ms"
                ;;
        esac
    done
done
