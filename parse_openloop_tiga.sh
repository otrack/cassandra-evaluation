#!/usr/bin/env bash

# Usage: ./parse_openloop_tiga.sh <file1> <file2> ... > output.csv
#
# Parses Tiga open-loop benchmark logs (produced by tiga_openloop.sh).  The
# per-DC files are named tiga_<nodes>_r<rate>_sw_<timestamp>[_<DC>].dat and
# contain:
#   [OL-SUMMARY]   aggregate pump results (completed/tput_hz/percentiles/avg)
#   [OL]           one raw latency sample per completed transaction
#   [COMMIT-PROBE] per-rate coordinator commit-path breakdown + perReplica
#                  arrival stats (fresh per client, i.e. per rate)
# The output rows keep the cdf.csv column layout (protocol..ephemeral_path) so
# existing consumers keep working, and append the commit-path/arrival probe
# columns after col 109.

DIR=$(dirname "${BASH_SOURCE[0]}")

source ${DIR}/utils.sh

# Output concise header (cdf-shaped + probe columns)
header="protocol,nodes,workload,conflict_rate,dc,op,clients,tput,avg_latency_us"
for p in $(seq 1 100); do
    header="$header,p$p"
done
header="$header,failed,fast_path,medium_path,slow_path,ephemeral_path"
header="$header,direct_commit,sync_commit,direct_commit_miss,sync_commit_miss,arrival_seen,arrival_missed,arrival_max_miss_us,arrival"
echo "$header"

# Process a single file, outputting one CSV row
process_file() {
    local file="$1"
    local filename
    filename=$(basename "$file")

    # Parse filename: <protocol>_<nodes>_r<rate>_sw[_<arrival>_]<timestamp>[_<DC>].dat
    if [[ "$filename" =~ ^([^_]+)_([0-9]+)_r([0-9]+)_sw_([a-z]+_)?([0-9]+)(_([A-Za-z]+))?\.dat$ ]]; then
        local protocol="${BASH_REMATCH[1]}"
        local nodes="${BASH_REMATCH[2]}"
        local rate="${BASH_REMATCH[3]}"
        local arrival="${BASH_REMATCH[4]}"
        arrival="${arrival%_}"
        arrival="${arrival:-deterministic}"
        local timestamp="${BASH_REMATCH[5]}"
        local dc="${BASH_REMATCH[7]}"
    else
        error "Ignoring ${filename}"
        return
    fi
    if [ -z "$dc" ]; then
        dc="total"
    fi

    # Latency samples sorted ascending.  One [OL] line per completed txn:
    # [OL] <latUs> <boundUs> <repSlow> <nonSerial>
    local lats
    lats=$(grep -E '^\[OL\] ' "$file" | awk '{print $2}' | sort -n)
    local n_lats
    n_lats=$(printf '%s\n' "$lats" | sed '/^$/d' | wc -l)
    n_lats=${n_lats//[[:space:]]/}
    if [ "$n_lats" -eq 0 ]; then
        error "No [OL] samples in ${filename}; skipping"
        return
    fi

    # Build the distribution row via a single awk that pulls the
    # other columns from the log file itself.
    awk -v protocol="$protocol" -v nodes="$nodes" \
        -v workload="sw" -v dc="$dc" -v rate="$rate" \
        -v arrival="$arrival" -v n_lats="$n_lats" '
    BEGIN {
        # first pass through lats via FNR == NR file
        n = 0
    }
    NR == FNR {
        lat[++n] = $1 + 0
        next
    }
    {
        # swap.s= is echoed in the YCSB log (used for cdf conflict_rate)
        if (conflict_rate == "" && match($0, /swap\.s=[0-9]+/)) {
            conflict_rate = substr($0, RSTART + 7, RLENGTH - 7)
        }
        if (conflict_rate == "" && match($0, /swapSize=[0-9]+/)) {
            conflict_rate = substr($0, RSTART + 8, RLENGTH - 8)
        }
        if (tput == "" && match($0, /tput_hz=[0-9.]+/)) {
            tput = substr($0, RSTART + 8, RLENGTH - 8)
        }
        if (avg == "" && match($0, /avg=[0-9.]+/)) {
            avg = substr($0, RSTART + 4, RLENGTH - 4)
        }
        if (/\[COMMIT-PROBE\] coordinatorId=/) {
            probe_line = $0
        }
    }
    END {
        if (conflict_rate == "") conflict_rate = "NA"
        if (tput == "") tput = "unknown"
        if (avg == "") avg = "unknown"

        dc_count = 0
        sc_count = 0
        dcm_count = 0
        scm_count = 0
        arr_seen = 0
        arr_missed = 0
        arr_max = 0
        if (probe_line != "") {
            nf = split(probe_line, pf, " ")
            cur_perrep = 0
            for (i = 1; i <= nf; i++) {
                if (pf[i] ~ /^directCommit=/) { sub(/^directCommit=/, "", pf[i]); dc_count = pf[i] + 0 }
                else if (pf[i] ~ /^syncCommit=/) { sub(/^syncCommit=/, "", pf[i]); sc_count = pf[i] + 0 }
                else if (pf[i] ~ /^directCommitMiss=/) { sub(/^directCommitMiss=/, "", pf[i]); dcm_count = pf[i] + 0 }
                else if (pf[i] ~ /^syncCommitMiss=/) { sub(/^syncCommitMiss=/, "", pf[i]); scm_count = pf[i] + 0 }
                else if (pf[i] ~ /^perReplica=/) { cur_perrep = 1 }
                else if (pf[i] ~ /^[a-zA-Z_]+=/) { cur_perrep = 0 }
                if (cur_perrep && pf[i] ~ /\{/) {
                    s = pf[i]
                    sub(/.*\{/, "", s)
                    sub(/\}.*/, "", s)
                    if (s ~ /^[0-9]+\/[0-9]+\/[0-9]+$/) {
                        split(s, t, "/")
                        arr_seen += t[1] + 0
                        arr_missed += t[2] + 0
                        if ((t[3] + 0) > arr_max) arr_max = t[3] + 0
                    }
                }
            }
        }

        row = protocol "," nodes "," workload "," conflict_rate "," dc \
              ",OPENLOOP," rate "," tput "," avg
        for (p = 1; p <= 100; p++) {
            rank = int(n_lats * p / 100) + 1
            if (rank < 1) rank = 1
            if (rank > n_lats) rank = n_lats
            row = row "," (rank in lat ? lat[rank] : "unknown")
        }
        row = row ",0,NA,NA,NA,NA" \
              "," dc_count "," sc_count "," dcm_count "," scm_count \
              "," arr_seen "," arr_missed "," arr_max \
              "," arrival
        print row
    }
    ' <(printf '%s\n' "$lats" | sed '/^$/d') "$file"
}

export -f process_file
export -f error
export -f debug

# Use GNU parallel if available, otherwise fall back to sequential processing
if command -v parallel >/dev/null 2>&1; then
    parallel --group process_file ::: "$@"
else
    for file in "$@"; do
        process_file "$file"
    done
fi