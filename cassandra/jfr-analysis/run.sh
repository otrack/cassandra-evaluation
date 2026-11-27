#!/usr/bin/env bash
# Compile (when needed) and run one of the JFR analysis tools in this directory.
#
#   cassandra/jfr-analysis/run.sh <Tool> <args...>
#   cassandra/jfr-analysis/run.sh SpanStats results/profiles/*.jfr
#
# Needs a JDK 17+ (records, jdk.jfr): JFR_JAVA_HOME if set; otherwise $JAVA_HOME, else java/javac on the PATH,
# unless that one is older -- JAVA_HOME is often pinned to the JDK a Cassandra build wants -- in which case the newest
# 17+ JDK installed (/usr/libexec/java_home on macOS, /usr/lib/jvm elsewhere) is used instead.
# JFR_HEAP sets the heap (default 8g): a minute of capture on a busy node is ~1M samples.
set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BUILD="${DIR}/build"

# The major version of the javac at $1, or nothing (a macOS /usr/bin stub with no JDK installed prints an error).
javac_major() {
    [ -x "$1" ] || return 0
    "$1" -version 2>&1 | awk '/^javac /{ split($2, v, "."); print (v[1] == "1" ? v[2] : v[1]) + 0; exit }'
}

find_javac() {
    if [ -n "${JFR_JAVA_HOME:-}" ]; then
        echo "${JFR_JAVA_HOME}/bin/javac"
        return
    fi
    local c
    {
        [ -n "${JAVA_HOME:-}" ] && echo "${JAVA_HOME}/bin/javac"
        command -v javac || true
        [ -x /usr/libexec/java_home ] && /usr/libexec/java_home -v 17+ 2>/dev/null | sed 's|$|/bin/javac|'
        ls -d /usr/lib/jvm/*/bin/javac 2>/dev/null | sort -rV
    } | while read -r c; do
        if [ "$(javac_major "${c}")" -ge 17 ] 2>/dev/null; then
            echo "${c}"
            break
        fi
    done
}

JAVAC=$(find_javac || true)
if [ -z "${JAVAC}" ] || ! [ "$(javac_major "${JAVAC}")" -ge 17 ] 2>/dev/null; then
    echo "$0: needs a JDK 17+ (found: $(javac_major "${JAVAC:-${JAVA_HOME:-}/bin/javac}" || true)${JFR_JAVA_HOME:+ at JFR_JAVA_HOME=${JFR_JAVA_HOME}});" >&2
    echo "  install one, or point JFR_JAVA_HOME at it (JAVA_HOME can stay on the JDK your Cassandra build needs)" >&2
    exit 1
fi
JAVA="$(dirname "${JAVAC}")/java"

if [ $# -lt 1 ]; then
    echo "usage: $0 <Tool> <args...>" >&2
    echo "tools: $(cd "${DIR}" && ls *.java | sed 's/\.java$//' | grep -vE '^(Tags|Dx|Threads)$' | paste -sd' ')" >&2
    exit 2
fi

tool=$1; shift
if [ ! -f "${DIR}/${tool}.java" ] || [[ "${tool}" =~ ^(Tags|Dx|Threads)$ ]]; then
    echo "no such tool: ${tool}" >&2
    exit 2
fi

# recompile everything if any source is newer than the build stamp (tools share Tags, Dx, Threads and Drain)
if [ ! -f "${BUILD}/.stamp" ] || [ -n "$(find "${DIR}" -maxdepth 1 -name '*.java' -newer "${BUILD}/.stamp")" ]; then
    mkdir -p "${BUILD}"
    "${JAVAC}" -encoding UTF-8 -nowarn -d "${BUILD}" "${DIR}"/*.java
    touch "${BUILD}/.stamp"
fi

exec "${JAVA}" -Xmx"${JFR_HEAP:-8g}" -cp "${BUILD}" "${tool}" "$@"
