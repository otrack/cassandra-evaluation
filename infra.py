#!/usr/bin/env python3
"""Python side of the infrastructure abstraction (see infra/README.md).

The shell scripts route every Docker operation through the d* wrappers in
utils.sh; the Python scripts route theirs through this module.  Both share the
same registry (latencies.csv) and the same node numbering:

    index = (dc - 1) * nodesperdc + k

where *dc* is the 1-based row of latencies.csv and *k* the 1-based node within
that DC.  Index 0 is the orchestrator, i.e. the local daemon.

In simulated mode (empty ``host`` column) every helper here returns the local
Docker client, so callers behave exactly as they did before this module
existed.
"""

import csv
import os
import re

import docker

_ROOT = os.path.dirname(os.path.abspath(__file__))

_CONFIG_CACHE = None
_REGISTRY_CACHE = None
_CLIENT_CACHE = {}


def load_config(path=None):
    """Parse exp.config into a dict, casting numeric values."""
    global _CONFIG_CACHE
    if _CONFIG_CACHE is not None and path is None:
        return _CONFIG_CACHE

    config = {}
    with open(path or os.path.join(_ROOT, "exp.config"), "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            value = value.strip()
            try:
                value = int(value)
            except ValueError:
                try:
                    value = float(value)
                except ValueError:
                    pass
            config[key.strip()] = value

    if path is None:
        _CONFIG_CACHE = config
    return config


def read_registry(path=None):
    """Rows of latencies.csv, one per DC, in order."""
    global _REGISTRY_CACHE
    if _REGISTRY_CACHE is not None and path is None:
        return _REGISTRY_CACHE

    rows = []
    with open(path or os.path.join(_ROOT, "latencies.csv"), newline="") as f:
        for row in csv.DictReader(f):
            rows.append({k: (v.strip().strip('"') if isinstance(v, str) else v)
                         for k, v in row.items() if k})

    if path is None:
        _REGISTRY_CACHE = rows
    return rows


def nodes_per_dc():
    return int(load_config().get("nodesperdc", 1) or 1)


def is_real():
    """True when at least one DC is backed by a real machine."""
    return any(row.get("host") for row in read_registry())


def dc_index_of(name):
    """1-based row of latencies.csv owning *name*, or 0 for the orchestrator."""
    if not name or name == "accord-viz":
        return 0
    if name in ("swiftpaxos-master", "ycsb"):
        return 1

    m = re.fullmatch(r"(?:ycsb-|database-node)(\d+)", name)
    if m:
        return int(m.group(1))

    m = re.fullmatch(r"([A-Za-z]+)(\d*)", name)
    if m:
        city = m.group(1)
        for i, row in enumerate(read_registry(), start=1):
            if row.get("loc") == city:
                return i
    return 0


def node_index_of(name):
    """Node index (1..N) owning *name*, or 0 for the orchestrator."""
    dc = dc_index_of(name)
    if dc == 0:
        return 0

    k = 1
    m = re.fullmatch(r"[A-Za-z]+(\d+)", name)
    if m:
        k = int(m.group(1))
    return (dc - 1) * nodes_per_dc() + k


def _row_for(name):
    dc = dc_index_of(name)
    if dc == 0:
        return None
    rows = read_registry()
    return rows[dc - 1] if dc <= len(rows) else None


def host_for(name):
    """(host, ssh_user) backing *name*, or (None, None) in simulated mode."""
    row = _row_for(name)
    if not row or not row.get("host"):
        return None, None
    return row["host"], row.get("ssh_user") or os.environ.get("USER")


def net_device(name):
    """Interface name to hand to tc for *name*."""
    row = _row_for(name)
    return (row.get("net_device") if row else None) or "eth0"


def client_for(name):
    """Docker client for the daemon owning *name*."""
    host, user = host_for(name)
    if not host:
        return docker.from_env()

    url = "ssh://{}@{}".format(user, host)
    if url not in _CLIENT_CACHE:
        try:
            _CLIENT_CACHE[url] = docker.DockerClient(base_url=url, use_ssh_client=True)
        except TypeError:
            # docker-py < 4.4 has no use_ssh_client and shells out to paramiko.
            _CLIENT_CACHE[url] = docker.DockerClient(base_url=url)
    return _CLIENT_CACHE[url]


def container_for(name):
    """The container object *name*, fetched from the daemon that owns it."""
    return client_for(name).containers.get(name)


def all_clients():
    """Every daemon taking part in the deployment, deduplicated."""
    if not is_real():
        return [docker.from_env()]

    clients, seen = [], set()
    for row in read_registry():
        host = row.get("host")
        if not host or host in seen:
            continue
        seen.add(host)
        clients.append(client_for(row["loc"] + "1"))
    return clients


def all_containers(**kwargs):
    """containers.list(**kwargs) across every daemon of the deployment."""
    found = []
    for client in all_clients():
        try:
            found.extend(client.containers.list(**kwargs))
        except Exception as exc:  # a machine may be down mid-teardown
            print("Warning: could not list containers: {}".format(exc))
    return found
