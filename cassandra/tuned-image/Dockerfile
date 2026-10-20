# 0track/cassandra-accord:tuned = 0track/cassandra-accord:latest with
#  - accord.queue_shard_count=16 (the default, cores/8 = 2 on 16 cores, makes the AccordExecutor locks the bottleneck),
#  - G1 instead of generational ZGC (throughput collapses beyond ~80 clients/site on calvin_ubench with ZGC),
#  - Apply's synchronous continuation sliced to the keys of its command store (accord-applylink-slice-keys.patch).
# Build with ./build.sh, which makes accord-patched.jar.
FROM 0track/cassandra-accord:latest
COPY accord-patched.jar /opt/cassandra/lib/cassandra-accord-6.0-alpha3-SNAPSHOT.jar
RUN set -eux; \
	sed -i 's/^ # queue_shard_count: -1/ queue_shard_count: 16/' /etc/cassandra/cassandra.yaml; \
	grep -q '^ queue_shard_count: 16' /etc/cassandra/cassandra.yaml; \
	sed -i -e 's/^-XX:+UseZGC$/-XX:+UseG1GC/' -e '/^-XX:+ZGenerational$/d' /etc/cassandra/jvm21-server.options; \
	grep -q '^-XX:+UseG1GC' /etc/cassandra/jvm21-server.options
