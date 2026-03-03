#!/usr/bin/env bash

# Usage: ./parse_ycsb_files_to_csv.sh <file1> <file2> ... > output.csv

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

# Output concise header
header="protocol,nodes,workload,conflict_rate,city,op,clients,tput"
for p in $(seq 1 100); do
    header="$header,p$p"
done
echo "$header"

# Process a single file, outputting CSV rows
process_file() {
    local file="$1"
    local filename
    filename=$(basename "$file")

    # Parse filename: <protocol>_<nodes>_<workload>_<timestamp>_<city>.dat
    if [[ "$filename" =~ ^([^_]+)_([0-9]+)_([^_]+)_([0-9]+)_([A-Za-z]+)\.dat$ ]]; then
        local protocol="${BASH_REMATCH[1]}"
        local nodes="${BASH_REMATCH[2]}"
        local workload="${BASH_REMATCH[3]}"
        local city="${BASH_REMATCH[5]}"
    else
        error "Ignoring ${filename}"
        return
    fi

    # Single awk pass: extract all needed values and generate CSV rows
    awk -v protocol="$protocol" -v nodes="$nodes" \
        -v workload="$workload" -v city="$city" '
    BEGIN {
        clients       = "unknown"
        conflict_rate = "NA"
        tput          = "unknown"
        is_conflict   = 0
    }

    # Extract clients: handle both "-threads 64" and "-threads=64"
    clients == "unknown" {
        for (i = 1; i <= NF; i++) {
            if ($i == "-threads") {
                clients = $(i+1)
                break
            }
            if (substr($i, 1, 9) == "-threads=" && length($i) > 9) {
                clients = substr($i, 10)
                break
            }
        }
    }

    # Detect ConflictWorkload
    /site\.ycsb\.workloads\.ConflictWorkload/ { is_conflict = 1 }

    # Extract conflict.theta (only once)
    is_conflict && conflict_rate == "NA" && /conflict\.theta=/ {
        if (match($0, /conflict\.theta=[0-9]+(\.[0-9]+)?/)) {
            conflict_rate = substr($0, RSTART + 15, RLENGTH - 15)
        }
    }

    # Extract overall throughput (first occurrence)
    tput == "unknown" && /^\[OVERALL\], Throughput\(ops\/sec\),/ {
        split($0, a, ",")
        t = a[3]
        gsub(/^[ \t]+|[ \t]+$/, "", t)
        if (t != "") tput = sprintf("%.2f", t + 0)
    }

    # Extract percentile latencies: "[OP], NNNthPercentileLatency(us), VALUE"
    /PercentileLatency\(us\),/ {
        split($0, parts, ",")
        # parts[1]="[OP]", parts[2]=" NNNthPercentileLatency(us)", parts[3]=" VALUE"

        op_field = parts[1]
        gsub(/^\[|\].*$/, "", op_field)
        op_lower = tolower(op_field)

        pct_field = parts[2]
        gsub(/^[ \t]+/, "", pct_field)
        # Require a letter immediately after the digits to skip decimal fields like "99.9PercentileLatency"
        if (!match(pct_field, /^[0-9]+[a-zA-Z]/)) next
        match(pct_field, /^[0-9]+/)
        p = substr(pct_field, 1, RLENGTH) + 0

        val = parts[3]
        gsub(/^[ \t]+|[ \t]+$/, "", val)
        if (p >= 1 && p <= 100 && val ~ /^[0-9]+$/) {
            lat[op_lower SUBSEP p] = int(val / 1000 + 0.5)
        }
    }

    END {
        n_ops = split("read insert update scan readmodifywrite tx-readmodifywrite", ops, " ")
        for (o = 1; o <= n_ops; o++) {
            op  = ops[o]
	    # remove all occurrences of "swiftpaxos-" from protocol for output
            protocol_clean = protocol
            gsub(/swiftpaxos-/, "", protocol_clean)
            row = protocol_clean "," nodes "," workload "," conflict_rate "," city \
                  "," op "," clients "," tput
            for (p = 1; p <= 100; p++) {
                key = op SUBSEP p
                row = row "," (key in lat ? lat[key] : "unknown")
            }
            # Only output if the last percentile (p100) is present
            if (row !~ /,unknown$/) print row
        }
    }
    ' "$file"
}

export -f process_file
export -f error

# Use GNU parallel if available, otherwise fall back to sequential processing
if command -v parallel >/dev/null 2>&1; then
    parallel --group process_file ::: "$@"
else
    for file in "$@"; do
        process_file "$file"
    done
fi
