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

for scope in rw ro; do
    attribute="50thPercentile"
    PREACCEPT=$(jmx_get "org.apache.cassandra.metrics:name=PreAcceptLatency,scope=$scope,type=AccordCoordinator" ${attribute})
    EXECUTE=$(jmx_get "org.apache.cassandra.metrics:name=ExecuteLatency,scope=$scope,type=AccordCoordinator" ${attribute}) # until stable
    APPLY=$(jmx_get "org.apache.cassandra.metrics:name=ApplyLatency,scope=$scope,type=AccordCoordinator" ${attribute}) # end-to-end
    echo "preaccept: ${PREACCEPT}"
    echo "execute: ${EXECUTE}"
    echo "apply: ${APPLY}"
done
