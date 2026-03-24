#!/bin/bash
set -e

CONTAINER_ID="${1:?Usage: $0 <container_id_or_name>}"
JMX_HOST="localhost:9010"
JMXTERM_JAR="/tmp/jmxterm-1.0.4-uber.jar"
JMXTERM_URL="https://github.com/jiaqi/jmxterm/releases/download/v1.0.4/jmxterm-1.0.4-uber.jar"

# Download jmxterm to the container if not already present
docker exec "$CONTAINER_ID" bash -c "test -f $JMXTERM_JAR || curl -sL -o $JMXTERM_JAR $JMXTERM_URL"

jmx_get() {
  local bean_name="$1"
  local attribute="$2"
  docker exec "$CONTAINER_ID" bash -c "
cat > /tmp/jmx_cmds.txt << INNER
domain org.apache.cassandra.metrics
bean $bean_name
get $attribute
INNER
java -jar $JMXTERM_JAR -l $JMX_HOST -i /tmp/jmx_cmds.txt -n
" 2>/dev/null | grep "^$attribute" | awk -F'=' '{print $2}' | sed s/\;//g 
}

attribute="50thPercentile"

# fast commit
FAST_COMMIT=$(jmx_get "org.apache.cassandra.metrics:name=PreAcceptLatency,scope=rw,type=AccordCoordinator" ${attribute})
echo "fast_commit:${FAST_COMMIT}"

# slow commit
PREACCEPT_REQ=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_PRE_ACCEPT_REQ-WaitLatency,type=Messaging" ${attribute}) 
PREACCEPT_RSP=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_PRE_ACCEPT_RSP-WaitLatency,type=Messaging" ${attribute})
PREACCEPT=$(echo ${PREACCEPT_REQ} + ${PREACCEPT_RSP} | bc)
ACCEPT_REQ=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_ACCEPT_REQ-WaitLatency,type=Messaging" ${attribute})
ACCEPT_RSP=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_ACCEPT_RSP-WaitLatency,type=Messaging" ${attribute})
SLOW_COMMIT=$(echo ${PREACCEPT_REQ} + ${PREACCEPT_RSP} + ${ACCEPT_REQ} + ${ACCEPT_RSP}  | bc)
echo "slow_commit:${SLOW_COMMIT}"

# ratios
FAST=$(jmx_get "org.apache.cassandra.metrics:name=FastPaths,scope=rw,type=AccordCoordinator" Count)
MEDIUM=$(jmx_get "org.apache.cassandra.metrics:name=MediumPaths,scope=rw,type=AccordCoordinator" Count)
SLOW=$(jmx_get "org.apache.cassandra.metrics:name=SlowPaths,scope=rw,type=AccordCoordinator" Count)
FAST=${FAST:-0}
MEDIUM=${MEDIUM:-0}
SLOW=${SLOW:-0}
TOTAL=$((FAST + MEDIUM + SLOW))
FAST_PATH_RATIO=$(awk "BEGIN {printf \"%.4f\", $FAST/$TOTAL}")

COMMIT=$(echo "${FAST_COMMIT} * ${FAST_PATH_RATIO} + ${SLOW_COMMIT} * (1 - ${FAST_PATH_RATIO})" | bc)
echo "commit:${COMMIT}"

EXECUTE=$(jmx_get "org.apache.cassandra.metrics:name=ExecuteLatency,scope=rw,type=AccordCoordinator" ${attribute}) # until stable
echo "ordering:$(echo "x=${EXECUTE} - ${COMMIT}; if (x <= 0) x=0; x" | bc)"

APPLY=$(jmx_get "org.apache.cassandra.metrics:name=ApplyLatency,scope=rw,type=AccordCoordinator" ${attribute}) # end-to-end
echo "execution:$(echo ${APPLY} - ${EXECUTE} | bc)"

