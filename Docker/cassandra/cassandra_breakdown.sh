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
       CONTAINER_ID=$(config "node_name")$i
       echo -n "$(cassandra_get_dc ${CONTAINER_ID}),"
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
       echo -n "${FAST_COMMIT// /},"

       # slow commit
       PREACCEPT_REQ=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_PRE_ACCEPT_REQ-WaitLatency,type=Messaging" ${attribute}) 
       PREACCEPT_RSP=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_PRE_ACCEPT_RSP-WaitLatency,type=Messaging" ${attribute})
       PREACCEPT=$(echo ${PREACCEPT_REQ} + ${PREACCEPT_RSP} | bc)
       ACCEPT_REQ=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_ACCEPT_REQ-WaitLatency,type=Messaging" ${attribute})
       ACCEPT_RSP=$(jmx_get "org.apache.cassandra.metrics:name=ACCORD_ACCEPT_RSP-WaitLatency,type=Messaging" ${attribute})
       SLOW_COMMIT=$(echo ${PREACCEPT_REQ} + ${PREACCEPT_RSP} + ${ACCEPT_REQ} + ${ACCEPT_RSP}  | bc)
       echo -n "${SLOW_COMMIT},"

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
       echo -n "${COMMIT},"

       EXECUTE=$(jmx_get "org.apache.cassandra.metrics:name=ExecuteLatency,scope=rw,type=AccordCoordinator" ${attribute}) # until stable
       echo -n "$(echo "x=${EXECUTE} - ${COMMIT}; if (x <= 0) x=0; x" | bc),"

       APPLY=$(jmx_get "org.apache.cassandra.metrics:name=ApplyLatency,scope=rw,type=AccordCoordinator" ${attribute}) # end-to-end
       echo "$(echo ${APPLY} - ${EXECUTE} | bc)"
       
   done
   
}
