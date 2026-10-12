#!/usr/bin/env bash
# Compile (when needed) and run one of the JFR analysis tools in this directory.
#
#   cassandra/jfr-analysis/run.sh <Tool> <args...>
#   cassandra/jfr-analysis/run.sh SpanStats results/profiles/*.jfr
#
# Needs a JDK 17+ (records, jdk.jfr). Uses $JAVA_HOME if set, else java/javac on the PATH.
# JFR_HEAP sets the heap (default 8g): a minute of capture on a busy node is ~1M samples.
set -euo pipefail

DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
BUILD="${DIR}/build"
JAVA="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
JAVAC="${JAVA_HOME:+${JAVA_HOME}/bin/}javac"

if [ $# -lt 1 ]; then
    echo "usage: $0 <Tool> <args...>" >&2
    echo "tools: $(cd "${DIR}" && ls *.java | sed 's/\.java$//' | grep -vE '^(Tags|Dx)$' | paste -sd' ')" >&2
    exit 2
fi

tool=$1; shift
if [ ! -f "${DIR}/${tool}.java" ] || [[ "${tool}" =~ ^(Tags|Dx)$ ]]; then
    echo "no such tool: ${tool}" >&2
    exit 2
fi

# recompile everything if any source is newer than the build stamp (tools share Tags, Dx and Drain)
if [ ! -f "${BUILD}/.stamp" ] || [ -n "$(find "${DIR}" -maxdepth 1 -name '*.java' -newer "${BUILD}/.stamp")" ]; then
    mkdir -p "${BUILD}"
    "${JAVAC}" -nowarn -d "${BUILD}" "${DIR}"/*.java
    touch "${BUILD}/.stamp"
fi

exec "${JAVA}" -Xmx"${JFR_HEAP:-8g}" -cp "${BUILD}" "${tool}" "$@"
