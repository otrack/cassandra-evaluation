#!/usr/bin/env python3
"""Python side of the infrastructure abstraction (see infra/README.md).

The shell scripts route every Docker operation through the d* wrappers in
utils.sh; the Python scripts route theirs through this module.  Both read the
same two files and share the same node numbering:

    index = (dc - 1) * nodesperdc + k

where *dc* is the 1-based row of the active provider's locations map and *k*
the 1-based node within that DC.  Index 0 is the orchestrator, i.e. the local
daemon.

  * the locations map (lat,lon,loc) says which places this provider deploys to;
    the simulation authors it by hand, a cloud provider derives it from the
    regions it runs in.
  * the deployment state (node,host,ssh_user,net_device) is written by
    provisioning and lives under .deployment/, which is not version controlled.

With no state recorded -- the simulated case -- every helper here returns the
local Docker client, so callers behave exactly as they did before this module
existed.
"""

import csv
import os

import docker

_ROOT = os.path.dirname(os.path.abspath(__file__))

_CONFIG_CACHE = None
_LOCATIONS_CACHE = None
_STATE_CACHE = None
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


def provider():
    return str(load_config().get("infra") or "simulation")


def machine_shape():
    """The VM/container shape for the active provider.

    `machine` sizes the containers the simulation packs onto one host; a cloud
    provider may need a different shape, set as `<provider>.machine`.
    """
    cfg = load_config()
    return str(cfg.get("%s.machine" % provider()) or cfg.get("machine") or "")


def locations_file():
    """Path to the active provider's lat,lon,loc map.

    A provider that derives its locations (a cloud one, joining its zone list
    against a region table) materialises them under .deployment/; one that
    authors them by hand ships them in its own directory.
    """
    derived = os.path.join(_ROOT, ".deployment", "%s.locations.csv" % provider())
    if os.path.exists(derived):
        return derived

    authored = os.path.join(_ROOT, "infra", provider(), "locations.csv")
    # A provider that derives its map ships something else here (a zone list,
    # say).  Handing that back would let callers parse zero locations and plot
    # silently without their theoretical bounds, so say what is wrong instead.
    with open(authored, newline="") as f:
        header = (f.readline() or "").strip().split(",")
    if not {"lat", "lon", "loc"} <= {h.strip() for h in header}:
        raise RuntimeError(
            "%s is not a lat,lon,loc map: provider '%s' derives its locations. "
            "Run any benchmark entry point (or ./deploy.sh status) once so that "
            "%s is materialised." % (authored, provider(), derived))
    return authored


def state_file():
    return os.path.join(_ROOT, ".deployment", "%s.csv" % provider())


def read_locations():
    """Rows of the locations map, one per DC, in order."""
    global _LOCATIONS_CACHE
    if _LOCATIONS_CACHE is not None:
        return _LOCATIONS_CACHE

    path = locations_file()
    rows = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            rows.append({k: (v.strip().strip('"') if isinstance(v, str) else v)
                         for k, v in row.items() if k})

    if rows and not {"lat", "lon", "loc"} <= set(rows[0]):
        raise RuntimeError(
            "%s is not a lat,lon,loc map. Provider '%s' derives its locations; "
            "run any benchmark entry point (or ./deploy.sh status) once so that "
            "it is materialised." % (path, provider()))

    _LOCATIONS_CACHE = rows
    return rows


def _read_state():
    global _STATE_CACHE
    if _STATE_CACHE is not None:
        return _STATE_CACHE

    rows = {}
    path = state_file()
    if os.path.exists(path):
        with open(path, newline="") as f:
            for row in csv.DictReader(f):
                node = (row.get("node") or "").strip()
                if node.isdigit():
                    rows[int(node)] = {k: (v.strip() if isinstance(v, str) else v)
                                       for k, v in row.items() if k}
    _STATE_CACHE = rows
    return rows


def nodes_per_dc():
    return int(load_config().get("nodesperdc", 1) or 1)


def is_real():
    """True when at least one node is backed by a real machine."""
    return any(row.get("host") for row in _read_state().values())


def dc_index_of(name):
    """1-based row of the locations map owning *name*, or 0 for the orchestrator."""
    if not name or name == "accord-viz":
        return 0
    if name in ("swiftpaxos-master", "ycsb"):
        return 1

    for prefix in ("ycsb-", "database-node"):
        if name.startswith(prefix):
            suffix = name[len(prefix):]
            return int(suffix) if suffix.isdigit() else 0

    city = "".join(c for c in name if not c.isdigit())
    if city:
        for i, row in enumerate(read_locations(), start=1):
            if row.get("loc") == city:
                return i
    return 0


def node_index_of(name):
    """Node index (1..N) owning *name*, or 0 for the orchestrator."""
    dc = dc_index_of(name)
    if dc == 0:
        return 0

    digits = "".join(c for c in name if c.isdigit())
    k = int(digits) if digits and not name.startswith(("ycsb-", "database-node")) else 1
    return (dc - 1) * nodes_per_dc() + k


def _state_of(name):
    return _read_state().get(node_index_of(name), {})


def host_for(name):
    """(host, ssh_user) backing *name*, or (None, None) with no deployment."""
    row = _state_of(name)
    host = row.get("host")
    if not host:
        return None, None
    return host, row.get("ssh_user") or os.environ.get("USER")


def net_device(name):
    """Interface name to hand to tc for *name*."""
    return _state_of(name).get("net_device") or "eth0"


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
    for node, row in sorted(_read_state().items()):
        host = row.get("host")
        if not host or host in seen:
            continue
        seen.add(host)
        dc = (node - 1) // nodes_per_dc()
        locations = read_locations()
        if dc < len(locations):
            clients.append(client_for(locations[dc]["loc"] + "1"))
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
