#!/usr/bin/env bash

CASSANDRA_DIR=$(dirname "${BASH_SOURCE[0]}")
source ${CASSANDRA_DIR}/cluster.sh
source ${CASSANDRA_DIR}/../utils.sh

compute_breakdown() {

   local node_count=$1
   local protocol=$2

    if [ $# -lt 2 ]; then
	echo "Usage: $0 <node_count> accord"
	exit 1
    fi
   
   if [[ "${protocol}" != "accord" ]];
   then
       error "Only works with Accord."
       exit 1
   fi
   
   for i in $(seq 1 $node_count); do
       location=$(get_location $i ${CASSANDRA_DIR}/../latencies.csv 2>/dev/null)
       if [ -n "$location" ] && docker inspect "${location}1" >/dev/null 2>&1; then
           CONTAINER_ID="${location}1"
       else
           CONTAINER_ID="$(config "node_name")$i"
       fi

       if ! docker inspect "$CONTAINER_ID" >/dev/null 2>&1; then
           continue
       fi

       echo -n "$(cassandra_get_dc ${CONTAINER_ID}),"
       JMX_HOST="localhost:9010"
       JMXTERM_JAR="/tmp/jmxterm-1.0.4-uber.jar"
       JMXTERM_URL="https://github.com/jiaqi/jmxterm/releases/download/v1.0.4/jmxterm-1.0.4-uber.jar"
       
       # Download jmxterm to the container if not already present
       docker exec "$CONTAINER_ID" bash -c "test -f $JMXTERM_JAR || curl -sL -o $JMXTERM_JAR $JMXTERM_URL" >/dev/null 2>&1 || true

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

       FAST_COMMIT=$(jmx_get "org.apache.cassandra.metrics:name=PreAcceptLatency,scope=rw,type=AccordCoordinator" ${attribute})
       FAST_COMMIT=${FAST_COMMIT// /}
       echo -n "${FAST_COMMIT:-0},"

       PREACCEPT_REQ=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_PRE_ACCEPT_REQ-WaitLatency,type=Messaging" ${attribute}) 
       PREACCEPT_RSP=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_PRE_ACCEPT_RSP-WaitLatency,type=Messaging" ${attribute})
       PREACCEPT_REQ=${PREACCEPT_REQ// /}
       PREACCEPT_RSP=${PREACCEPT_RSP// /}
       PREACCEPT=$(echo "${PREACCEPT_REQ:-0} + ${PREACCEPT_RSP:-0}" | bc 2>/dev/null || echo 0)

       ACCEPT_REQ=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_ACCEPT_REQ-WaitLatency,type=Messaging" ${attribute})
       ACCEPT_RSP=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_ACCEPT_RSP-WaitLatency,type=Messaging" ${attribute})
       ACCEPT_REQ=${ACCEPT_REQ// /}
       ACCEPT_RSP=${ACCEPT_RSP// /}
       SLOW_COMMIT=$(echo "${PREACCEPT_REQ:-0} + ${PREACCEPT_RSP:-0} + ${ACCEPT_REQ:-0} + ${ACCEPT_RSP:-0}" | bc 2>/dev/null || echo 0)
       echo -n "${SLOW_COMMIT},"
       
       COMMIT=$(jmx_get "org.apache.cassandra.metrics:name=CommitLatency,scope=rw,type=AccordCoordinator" ${attribute})
       COMMIT=${COMMIT// /}
       echo -n "${COMMIT:-0},"
       
       EXECUTE=$(jmx_get "org.apache.cassandra.metrics:name=ExecuteLatency,scope=rw,type=AccordCoordinator" ${attribute})
       EXECUTE=${EXECUTE// /}
       echo -n "$(echo "x=${EXECUTE:-0} - ${COMMIT:-0}; if (x <= 0) x=0; x" | bc 2>/dev/null || echo 0),"

       APPLY=$(jmx_get "org.apache.cassandra.metrics:name=ApplyLatency,scope=rw,type=AccordCoordinator" ${attribute})
       APPLY=${APPLY// /}
       echo "$(echo "${APPLY:-0} - ${EXECUTE:-0}" | bc 2>/dev/null || echo 0)"
       
   done
   
}
