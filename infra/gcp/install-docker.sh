#!/bin/bash
#
# Startup script handed to every benchmark VM through instance metadata.
# Installs Docker and the tooling the experiments need inside the containers'
# host, then drops a marker file that infra_provision polls for.

set -e

MARKER=/var/lib/bench-node-ready

if [ -f "${MARKER}" ]; then
    exit 0
fi

export DEBIAN_FRONTEND=noninteractive

apt-get update -y
# The experiments run tc inside the containers (docker exec), so the images --
# not this host -- supply the binary they need.  iproute2 is installed here
# only so that a human can inspect or clear the VM's qdiscs by hand: under
# host networking a rule added by a container is applied to the VM's own
# interface and outlives that container.
apt-get install -y ca-certificates curl iproute2

if ! command -v docker >/dev/null 2>&1; then
    curl -fsSL https://get.docker.com | sh
fi

# Created ahead of any human user so that adding them to it later is enough.
groupadd -f docker

systemctl enable --now docker

# The benchmark starts long-lived JVMs; raise the limits Cassandra complains
# about rather than leaving warnings in every log.
cat > /etc/security/limits.d/99-bench.conf <<'EOF'
*  soft  nofile  1048576
*  hard  nofile  1048576
*  soft  memlock unlimited
*  hard  memlock unlimited
EOF

sysctl -w vm.max_map_count=1048575 >/dev/null
echo "vm.max_map_count=1048575" > /etc/sysctl.d/99-bench.conf

touch "${MARKER}"
