#!/usr/bin/env bash

# Usage: ./parse_ycsb_files_to_csv.sh <file1> <file2> ... > output.csv

# Helper to get correct English ordinal
ordinal_suffix() {
    n=$1
    if (( n % 100 >= 11 && n % 100 <= 13 )); then
        echo "${n}th"
    else
        case $((n % 10)) in
            1) echo "${n}st" ;;
            2) echo "${n}nd" ;;
            3) echo "${n}rd" ;;
            *) echo "${n}th" ;;
        esac
    fi
}

# Output concise header (added conflict_rate column)
header="protocol,nodes,workload,conflict_rate,phase,op,clients,tput"
for p in $(seq 1 100); do
    header="$header,p$p"
done
echo "$header"

for file in "$@"; do
    filename=$(basename "$file")

    # Parse filename with a pattern that matches the conflict.sh output naming:
    # <protocol>_<nodes>_[load|run]_<workload>_<conflict>.dat
    # Example: accord_3_run_a_0.1.dat
    if [[ "$filename" =~ ^([^_]+)_([0-9]+)_(load|run)_([^_]+)_([0-9]+(\.[0-9]+)?)\.dat$ ]]; then
        protocol="${BASH_REMATCH[1]}"
        nodes="${BASH_REMATCH[2]}"
        phase="${BASH_REMATCH[3]}"
        workload="${BASH_REMATCH[4]}"
        conflict="${BASH_REMATCH[5]}"
    else
        # Fallback to the older best-effort parsing if the strict pattern didn't match
        if [[ "$filename" =~ ^([^_]+)_([0-9]+)_(load|run)_([^\.]+)\.dat$ ]]; then
            protocol="${BASH_REMATCH[1]}"
            nodes="${BASH_REMATCH[2]}"
            phase="${BASH_REMATCH[3]}"
            workload="${BASH_REMATCH[4]}"
        else
            protocol="unknown"
            nodes="unknown"
            phase="unknown"
            workload="unknown"
        fi

        # Try to extract a conflict rate from the filename (several common patterns)
        conflict="unknown"
        if [[ "$filename" =~ [._-]conflict[_-]?([0-9]+(\.[0-9]+)?) ]]; then
            conflict="${BASH_REMATCH[1]}"
        elif [[ "$filename" =~ [._-]cr[_-]?([0-9]+(\.[0-9]+)?) ]]; then
            conflict="${BASH_REMATCH[1]}"
        else
            # fallback: take the first 0..1 decimal or integer match (avoid matching node counts which are integers >1)
            if [[ "$filename" =~ ([01](\.[0-9]+)?) ]]; then
                cand="${BASH_REMATCH[1]}"
                if [[ "$cand" == "0" || "$cand" == "1" || "$cand" == *.* ]]; then
                    conflict="$cand"
                fi
            fi
        fi
    fi

    # Extract clients (threads) from the file
    clients=$(grep -E 'Threads:|threadcount' "$file" | head -1 | grep -oE '[0-9]+')
    if [ -z "$clients" ]; then
        clients=$(grep -oE '\-threads[ =][0-9]+' "$file" | head -1 | grep -oE '[0-9]+')
    fi
    if [ -z "$clients" ]; then
        clients="unknown"
    fi

    # Extract overall throughput and truncate to two digits after the dot
    tput=$(grep '^\[OVERALL\], Throughput(ops/sec),' "$file" | head -1 | awk -F, '{print $3}' | xargs)
    if [ -z "$tput" ]; then
        tput="unknown"
    else
        tput=$(awk -v t="$tput" 'BEGIN { printf "%.2f", t }')
    fi

    for op in read insert update scan readmodifywrite; do
        op_upper=$(echo "$op" | awk '{print toupper($0)}')
        # include conflict in the row right after workload
        row="$protocol,$nodes,$workload,$conflict,$phase,$op,$clients,$tput"

        for p in $(seq 1 100); do
            ord=$(ordinal_suffix $p)
            # Try both upper and lower case for op match
            val=$(grep -E "^\[$op_upper\], ${ord}PercentileLatency\(us\)," "$file" | awk -F, '{print $3}' | xargs)
            if [ -z "$val" ]; then
                val=$(grep -E "^\[$op\], ${ord}PercentileLatency\(us\)," "$file" | awk -F, '{print $3}' | xargs)
            fi
            if [ -z "$val" ]; then
                row="$row,unknown"
            else
                latency_ms=$(awk "BEGIN { print int( ($val/1000) + 0.5 ) }")
                row="$row,$latency_ms"
            fi
        done

        # Only output if at least one percentile exists for this op in this file
        if echo "$row" | grep -vq ",unknown$"; then
            echo "$row"
        fi
    done
done
