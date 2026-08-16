"""Deployment topology as seen from INSIDE the Compose network.

Every URI here uses a Compose service hostname and directConnection=true, so a
read is served by exactly that node regardless of read preference. These URIs
are not reachable from the host - mellow_cli/deployment.py covers that case.
"""

from pymongo import MongoClient

DEPLOYMENTS = {
    "single": {
        "compose": "docker-compose.yml",
        "nodes": [("mongodb", 27017)],
    },
    "rs3": {
        "compose": "docker-compose.replicaset.yml",
        "nodes": [("mongo-primary", 27017),
                  ("mongo-secondary-1", 27018),
                  ("mongo-secondary-2", 27019)],
    },
    "rs5": {
        "compose": "docker-compose.replicaset5.yml",
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


def node_uri(host, port):
    return f"mongodb://{host}:{port}/?directConnection=true"


def node_count(name):
    return len(_config(name)["nodes"])


def all_node_uris(name):
    return [node_uri(host, port) for host, port in _config(name)["nodes"]]


def primary_uri(name, client_factory=MongoClient):
    """Ask the seed node who the primary is, so an election cannot mislead us."""
    nodes = _config(name)["nodes"]
    if name == "single":
        return node_uri(*nodes[0])
    seed = node_uri(*nodes[0])
    client = client_factory(seed, serverSelectionTimeoutMS=5000)
    hello = client.admin.command("hello")
    host, port = hello["primary"].rsplit(":", 1)
    return node_uri(host, int(port))


def secondary_uris(name, client_factory=MongoClient):
    if node_count(name) == 1:
        return []
    primary = primary_uri(name, client_factory=client_factory)
    return [uri for uri in all_node_uris(name) if uri != primary]


def server_version(uri, client_factory=MongoClient):
    try:
        client = client_factory(uri, serverSelectionTimeoutMS=5000)
        return client.admin.command("buildInfo")["version"]
    except Exception:
        return ""
