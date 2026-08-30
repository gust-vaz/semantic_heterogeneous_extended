"""Deployment topology as seen from INSIDE the Compose network.

Two kinds of URI live here, and the difference matters:

* `node_uri` / `all_node_uris` / `secondary_uris` are **read** URIs. They carry
  directConnection=true so a read is served by exactly that node regardless of
  read preference - that pinning is what the read-offloading experiments
  measure.
* `write_uri` is the **write** URI. It must NOT be pinned: a replica set can
  elect a new primary at any moment, and a pinned client cannot follow it
  (pymongo maps directConnection=true to TopologyType Single, which never
  rediscovers). Pinned writes turn any mid-run step-down into an
  unrecoverable NotPrimaryError instead of a retry against the new primary.

These URIs are not reachable from the host - mellow_cli/deployment.py covers
that case.
"""

from pymongo import MongoClient

DEPLOYMENTS = {
    "single": {
        "compose": "docker-compose.yml",
        "replica_set": None,
        "nodes": [("mongodb", 27017)],
    },
    "rs3": {
        "compose": "docker-compose.replicaset.yml",
        "replica_set": "rs0",
        "nodes": [("mongo-primary", 27017),
                  ("mongo-secondary-1", 27018),
                  ("mongo-secondary-2", 27019)],
    },
    "rs5": {
        "compose": "docker-compose.replicaset5.yml",
        "replica_set": "rs0",
        "nodes": [("mongo-primary", 27017),
                  ("mongo-secondary-1", 27018),
                  ("mongo-secondary-2", 27019),
                  ("mongo-secondary-3", 27020),
                  ("mongo-secondary-4", 27021)],
    },
}


def _config(name):
    if name not in DEPLOYMENTS:
        raise ValueError(
            f"Unknown deployment '{name}'. Choose: {', '.join(DEPLOYMENTS)}"
        )
    return DEPLOYMENTS[name]


def compose_file(name):
    return _config(name)["compose"]


def replica_set_name(name):
    """The set name, or None for a standalone deployment."""
    return _config(name)["replica_set"]


def node_uri(host, port):
    return f"mongodb://{host}:{port}/?directConnection=true"


def node_count(name):
    return len(_config(name)["nodes"])


def all_node_uris(name):
    return [node_uri(host, port) for host, port in _config(name)["nodes"]]


def write_uri(name):
    """Where writes go: a seed list naming the whole set, so the driver follows elections.

    Deliberately built from static config rather than discovery - the point is
    that this URI stays correct across a step-down, so there is nothing to
    discover. A standalone deployment has no set to follow and stays direct.
    """
    config = _config(name)
    if config["replica_set"] is None:
        return node_uri(*config["nodes"][0])
    hosts = ",".join(f"{host}:{port}" for host, port in config["nodes"])
    return f"mongodb://{hosts}/?replicaSet={config['replica_set']}"


def current_primary(name, client_factory=MongoClient):
    """(host, port) of the node that is primary right now.

    A point-in-time answer, only good for deciding which nodes to aim reads at.
    Never build a write URI from it - see the module docstring.
    """
    config = _config(name)
    if config["replica_set"] is None:
        return config["nodes"][0]
    seed = node_uri(*config["nodes"][0])
    client = client_factory(seed, serverSelectionTimeoutMS=5000)
    hello = client.admin.command("hello")
    host, port = hello["primary"].rsplit(":", 1)
    return host, int(port)


def secondary_uris(name, client_factory=MongoClient):
    """Read URIs for every node that is not currently the primary.

    Compared by (host, port) rather than by URI string: the primary is not
    necessarily the node named `mongo-primary`, and the write URI is no longer
    a per-node URI to diff against.
    """
    if node_count(name) == 1:
        return []
    primary = current_primary(name, client_factory=client_factory)
    return [node_uri(host, port)
            for host, port in _config(name)["nodes"]
            if (host, port) != primary]


def server_version(uri, client_factory=MongoClient):
    try:
        client = client_factory(uri, serverSelectionTimeoutMS=5000)
        return client.admin.command("buildInfo")["version"]
    except Exception:
        return ""
