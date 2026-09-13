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


class _FakeRouter:
    """A mongos as measured on MongoDB 8.0.12: its hello carries msg 'isdbgrid' and
    isWritablePrimary True, and no 'primary' or 'setName' - from the moment the
    config server has a primary, including while no shard is registered yet."""

    def __init__(self, registered_shards):
        self._registered = registered_shards
        self.admin = self

    def command(self, name):
        assert name == "hello"
        return {"msg": "isdbgrid", "isWritablePrimary": True, "ok": 1.0}

    def __getitem__(self, db_name):
        assert db_name == "config"
        return _FakeConfigDatabase(self._registered)


class _FakeConfigDatabase:
    def __init__(self, registered):
        self.shards = _FakeShardsCollection(registered)


class _FakeShardsCollection:
    def __init__(self, registered):
        self._registered = registered

    def count_documents(self, filter_):
        assert filter_ == {}
        return self._registered


@pytest.mark.parametrize("name", ["sh4", "sh8"])
def test_primary_uri_sharded_is_the_router_port(name):
    # A router's hello has no 'primary' to read a port from.
    d = Deployment(name, runner=lambda *a, **k: None)
    uri = d.primary_uri(client_factory=lambda uri, **kw: _FakeRouter(registered_shards=0))
    assert uri == "mongodb://localhost:27017/?directConnection=true"


def test_sharded_deployment_is_not_ready_before_any_shard_is_registered():
    # Measured: the router already answers isWritablePrimary=True while
    # config.shards is empty. Declaring it ready then hands the session a
    # cluster with nowhere to put data.
    d = Deployment("sh4", runner=lambda *a, **k: None)
    with pytest.raises(TimeoutError):
        d.wait_until_ready(timeout=0.1,
                           client_factory=lambda uri, **kw: _FakeRouter(registered_shards=0))


def test_sharded_deployment_is_not_ready_with_only_some_shards_registered():
    d = Deployment("sh8", runner=lambda *a, **k: None)
    with pytest.raises(TimeoutError):
        d.wait_until_ready(timeout=0.1,
                           client_factory=lambda uri, **kw: _FakeRouter(registered_shards=4))


def test_sharded_deployment_is_ready_once_every_shard_is_registered():
    d = Deployment("sh8", runner=lambda *a, **k: None)
    d.wait_until_ready(timeout=5,
                       client_factory=lambda uri, **kw: _FakeRouter(registered_shards=8))


def test_up_runs_compose_up_for_sh8_against_a_real_file():
    calls = []
    d = Deployment("sh8", runner=lambda cmd, **kw: calls.append(cmd))
    d.up(wait=False)
    assert calls == [["docker", "compose", "-f", str(d.compose_path()), "up", "-d"]]
    assert d.compose_path().is_file()
