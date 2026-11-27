#!/bin/bash
set -e

FAST_PATH_DIR=$(dirname "${BASH_SOURCE[0]}")
source ${FAST_PATH_DIR}/../utils.sh

CONTAINER_ID="${1:?Usage: $0 <container_id_or_name>}"
JMX_HOST="localhost:9010"
JMXTERM_JAR="/tmp/jmxterm-1.0.4-uber.jar"
JMXTERM_URL="https://github.com/jiaqi/jmxterm/releases/download/v1.0.4/jmxterm-1.0.4-uber.jar"

# Every counter is read in a single `docker exec`: on a real deployment each
# exec is an SSH round trip to another region, and this used to make one per
# counter (seven per node, one node after another).  Inside the container, one
# jmxterm per bean, each preceded by a marker line, so that a bean this build
# lacks (Ephemeral, say) leaves a gap rather than shifting the values of the
# others.  The values are parsed here with awk: BSD grep (macOS) has no -P.
BEANS="rw_fast:name=FastPaths,scope=rw rw_medium:name=MediumPaths,scope=rw rw_slow:name=SlowPaths,scope=rw \
ro_fast:name=FastPaths,scope=ro ro_medium:name=MediumPaths,scope=ro ro_slow:name=SlowPaths,scope=ro \
ro_ephemeral:name=Ephemeral,scope=ro"

counters=$(dexec "$CONTAINER_ID" sh -c "
test -f $JMXTERM_JAR || curl -sL -o $JMXTERM_JAR $JMXTERM_URL
for b in $BEANS; do
  echo \"BEAN \${b%%:*}\"
  printf 'domain org.apache.cassandra.metrics\nbean org.apache.cassandra.metrics:%s,type=AccordCoordinator\nget Count\n' \"\${b#*:}\" > /tmp/jmx_cmds.txt
  java -jar $JMXTERM_JAR -l $JMX_HOST -i /tmp/jmx_cmds.txt -n 2>/dev/null
done
" 2>/dev/null) || true

# The value of counter $1 (rw_fast, ...), 0 if it could not be read.
count() {
  awk -v k="$1" '$1 == "BEAN" { cur = $2; next }
                 cur == k && $1 == "Count" { gsub(/[^0-9]/, "", $3); print $3 + 0; found = 1; exit }
                 END { if (!found) print 0 }' <<< "${counters}"
}

for scope in rw ro; do
  FAST=$(count ${scope}_fast)
  MEDIUM=$(count ${scope}_medium)
  SLOW=$(count ${scope}_slow)

  if [ "$scope" = "ro" ]; then
    EPHEMERAL=$(count ro_ephemeral)
    SCOPE_TOTAL=$((FAST + MEDIUM + SLOW + EPHEMERAL))
    if [ "$SCOPE_TOTAL" -gt 0 ]; then
	echo "Ephemeral ratio: $(awk "BEGIN {printf \"%.4f\", $EPHEMERAL/$SCOPE_TOTAL}")"
    fi
  else
    SCOPE_TOTAL=$((FAST + MEDIUM + SLOW))
    if [ "$SCOPE_TOTAL" -gt 0 ]; then
	echo "Fast ratio: $(awk "BEGIN {printf \"%.4f\", $FAST/$SCOPE_TOTAL}")"
	echo "Medium ratio: $(awk "BEGIN {printf \"%.4f\", $MEDIUM/$SCOPE_TOTAL}")"
	echo "Slow ratio: $(awk "BEGIN {printf \"%.4f\", $SLOW/$SCOPE_TOTAL}")"
    fi
  fi
done
