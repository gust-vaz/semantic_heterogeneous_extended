from pathlib import Path
import pytest
from mellow_cli.deployment import Deployment, DEPLOYMENTS, node_uri, primary_port_from_hello


def test_node_uri_is_direct_by_port():
    assert node_uri(27018) == "mongodb://localhost:27018/?directConnection=true"


def test_primary_port_parsed_from_hello():
    hello = {"primary": "mongo-primary:27017",
             "hosts": ["mongo-primary:27017", "mongo-secondary-1:27018"]}
    assert primary_port_from_hello(hello) == 27017


def test_unknown_deployment_rejected():
    with pytest.raises(ValueError):
        Deployment("rs99")


def test_up_runs_compose_up_for_rs3():
    calls = []
    d = Deployment("rs3", runner=lambda cmd, **kw: calls.append(cmd))
    d.up(wait=False)
    assert calls == [["docker", "compose", "-f", str(d.compose_path()),
                      "up", "-d"]]
    assert d.compose_path().name == "docker-compose.replicaset.yml"


def test_down_purges_volumes_by_default():
    calls = []
    d = Deployment("rs5", runner=lambda cmd, **kw: calls.append(cmd))
    d.down()
    assert calls[0][:4] == ["docker", "compose", "-f", str(d.compose_path())]
    assert calls[0][-2:] == ["down", "-v"]


class _FakeClient:
    def __init__(self, hello):
        self._hello = hello
        self.admin = self

    def command(self, name):
        assert name == "hello"
        return self._hello


def test_primary_uri_single_is_27017():
    d = Deployment("single", runner=lambda *a, **k: None)
    assert d.primary_uri() == "mongodb://localhost:27017/?directConnection=true"


def test_primary_uri_rs_discovers_by_port():
    d = Deployment("rs3", runner=lambda *a, **k: None)
    hello = {"primary": "mongo-primary:27017", "isWritablePrimary": True,
             "hosts": ["mongo-primary:27017", "mongo-secondary-1:27018"]}
    uri = d.primary_uri(client_factory=lambda uri, **kw: _FakeClient(hello))
    assert uri == "mongodb://localhost:27017/?directConnection=true"
