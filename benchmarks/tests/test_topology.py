import pytest
from benchmarks.harness import topology


class FakeAdmin:
    def __init__(self, hello):
        self._hello = hello

    def command(self, name):
        assert name in ("hello", "buildInfo")
        if name == "hello":
            return self._hello
        return {"version": "8.0.12"}


class FakeClient:
    def __init__(self, uri, **kwargs):
        self.uri = uri
        self.admin = FakeAdmin({
            "primary": "mongo-secondary-1:27018",
            "isWritablePrimary": False,
        })


def test_deployment_names_and_compose_files():
    assert topology.compose_file("single") == "docker-compose.yml"
    assert topology.compose_file("rs3") == "docker-compose.replicaset.yml"
    assert topology.compose_file("rs5") == "docker-compose.replicaset5.yml"


def test_unknown_deployment_is_rejected():
    with pytest.raises(ValueError) as exc:
        topology.compose_file("rs9")
    assert "rs9" in str(exc.value)


def test_node_counts():
    assert topology.node_count("single") == 1
    assert topology.node_count("rs3") == 3
    assert topology.node_count("rs5") == 5


def test_node_uri_is_a_direct_connection():
    assert topology.node_uri("mongo-secondary-1", 27018) == (
        "mongodb://mongo-secondary-1:27018/?directConnection=true"
    )


def test_all_node_uris_use_service_hostnames_not_localhost():
    uris = topology.all_node_uris("rs3")
    assert uris == [
        "mongodb://mongo-primary:27017/?directConnection=true",
        "mongodb://mongo-secondary-1:27018/?directConnection=true",
        "mongodb://mongo-secondary-2:27019/?directConnection=true",
    ]
    assert all("localhost" not in uri for uri in uris)


def test_rs5_lists_all_five_members():
    assert len(topology.all_node_uris("rs5")) == 5
    assert "mongodb://mongo-secondary-4:27021/?directConnection=true" in topology.all_node_uris("rs5")


def test_single_primary_needs_no_discovery():
    assert topology.primary_uri("single") == (
        "mongodb://mongodb:27017/?directConnection=true"
    )


def test_primary_uri_follows_hello_even_after_an_election():
    # hello() reports mongo-secondary-1 as primary; we must believe it.
    assert topology.primary_uri("rs3", client_factory=FakeClient) == (
        "mongodb://mongo-secondary-1:27018/?directConnection=true"
    )


def test_secondary_uris_exclude_the_discovered_primary():
    secondaries = topology.secondary_uris("rs3", client_factory=FakeClient)
    assert "mongodb://mongo-secondary-1:27018/?directConnection=true" not in secondaries
    assert len(secondaries) == 2


def test_single_has_no_secondaries():
    assert topology.secondary_uris("single") == []


def test_server_version_is_read_from_buildinfo():
    assert topology.server_version("mongodb://x:1", client_factory=FakeClient) == "8.0.12"


def test_rs5_compose_defines_a_runner_service():
    # Plain-text assertions on purpose: PyYAML is not a declared dependency and
    # this only needs to catch the service being missing entirely.
    with open("docker-compose.replicaset5.yml") as handle:
        text = handle.read()
    assert "\n  runner:" in text
    assert "build: ./docker/runner" in text
    assert "working_dir: /app" in text
    assert "mellow-rs5-net" in text
