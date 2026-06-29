import subprocess
import time
from pathlib import Path
from pymongo import MongoClient

REPO_ROOT = Path(__file__).resolve().parent.parent

DEPLOYMENTS = {
    "single": {"compose": "docker-compose.yml", "ports": [27017]},
    "rs3": {"compose": "docker-compose.replicaset.yml", "ports": [27017, 27018, 27019]},
    "rs5": {"compose": "docker-compose.replicaset5.yml",
            "ports": [27017, 27018, 27019, 27020, 27021]},
}


def node_uri(port):
    return f"mongodb://localhost:{port}/?directConnection=true"


def primary_port_from_hello(hello):
    return int(hello["primary"].rsplit(":", 1)[1])


class Deployment:
    def __init__(self, name, runner=subprocess.run, repo_root=REPO_ROOT):
        if name not in DEPLOYMENTS:
            raise ValueError(f"Unknown deployment '{name}'. Choose: {', '.join(DEPLOYMENTS)}")
        self.name = name
        self.config = DEPLOYMENTS[name]
        self._runner = runner
        self.repo_root = Path(repo_root)

    def compose_path(self):
        return self.repo_root / self.config["compose"]

    def _compose(self, *args):
        self._runner(["docker", "compose", "-f", str(self.compose_path()), *args], check=True)

    def up(self, wait=True, timeout=90):
        self._compose("up", "-d")
        if wait:
            self.wait_until_ready(timeout=timeout)

    def primary_uri(self, client_factory=MongoClient):
        if self.name == "single":
            return node_uri(self.config["ports"][0])
        seed = node_uri(self.config["ports"][0])
        client = client_factory(seed, serverSelectionTimeoutMS=3000)
        hello = client.admin.command("hello")
        return node_uri(primary_port_from_hello(hello))

    def wait_until_ready(self, timeout=90, client_factory=MongoClient):
        seed = node_uri(self.config["ports"][0])
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                client = client_factory(seed, serverSelectionTimeoutMS=2000)
                hello = client.admin.command("hello")
                if self.name == "single" or hello.get("isWritablePrimary") or hello.get("primary"):
                    return
            except Exception:
                pass
            time.sleep(1.0)
        raise TimeoutError(f"Deployment '{self.name}' not ready after {timeout}s")

    def down(self, purge=True):
        args = ["down", "-v"] if purge else ["down"]
        self._compose(*args)
