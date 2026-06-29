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
