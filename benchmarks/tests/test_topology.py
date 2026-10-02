import pytest
from pymongo.settings import TopologySettings
from pymongo.topology_description import TOPOLOGY_TYPE
from pymongo.uri_parser import parse_uri

from benchmarks.harness import topology
from benchmarks.tests.compose_config import command, needs_docker, resolved


class FakeAdmin:
    def __init__(self, hello):
        self._hello = hello

    def command(self, name):
        assert name in ("hello", "buildInfo")
        if name == "hello":
            return self._hello
        return {"version": "8.0.12"}


class FakeClient:
    """A set whose current primary is mongo-secondary-1, not mongo-primary."""

    def __init__(self, uri, **kwargs):
        self.uri = uri
        self.admin = FakeAdmin({
            "primary": "mongo-secondary-1:27018",
            "isWritablePrimary": False,
        })


def topology_type_of(uri):
    """What pymongo itself makes of this URI.

    Single means the driver talks to exactly one server and never rediscovers:
    a step-down there is unrecoverable, not a failover.
    """
    parsed = parse_uri(uri)
    options = parsed["options"]
    return TopologySettings(
        seeds=parsed["nodelist"],
        replica_set_name=options.get("replicaset"),
        direct_connection=options.get("directconnection", False),
    ).get_topology_type()


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


def test_read_uris_stay_pinned_to_exactly_one_node():
    # B1 measures which node served the reads, so a read URI must never fan out.
    assert topology_type_of(topology.node_uri("mongo-secondary-1", 27018)) == (
        TOPOLOGY_TYPE.Single
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


def test_write_uri_is_never_pinned_to_one_node():
    # The regression this guards: a pinned write URI cannot follow an election,
    # so the first step-down mid-run fails every remaining write with
    # NotPrimaryError instead of retrying against the new primary.
    assert topology_type_of(topology.write_uri("rs3")) != TOPOLOGY_TYPE.Single
    assert topology_type_of(topology.write_uri("rs5")) != TOPOLOGY_TYPE.Single


def test_write_uri_seeds_every_member_and_names_the_replica_set():
    parsed = parse_uri(topology.write_uri("rs3"))
    assert parsed["nodelist"] == [
        ("mongo-primary", 27017),
        ("mongo-secondary-1", 27018),
        ("mongo-secondary-2", 27019),
    ]
    assert parsed["options"]["replicaset"] == "rs0"
    assert "directconnection" not in parsed["options"]


def test_rs5_write_uri_seeds_all_five_members():
    assert len(parse_uri(topology.write_uri("rs5"))["nodelist"]) == 5


def test_single_write_uri_stays_a_direct_connection():
    # One mongod, no replica set, no election to follow.
    assert topology.write_uri("single") == (
        "mongodb://mongodb:27017/?directConnection=true"
    )


def test_current_primary_is_read_from_hello():
    assert topology.current_primary("rs3", client_factory=FakeClient) == (
        "mongo-secondary-1", 27018
    )


def test_secondary_uris_exclude_whichever_node_is_primary_now():
    # hello() names mongo-secondary-1 as primary, so the node merely *called*
    # mongo-primary is a read target and mongo-secondary-1 is not.
    assert topology.secondary_uris("rs3", client_factory=FakeClient) == [
        "mongodb://mongo-primary:27017/?directConnection=true",
        "mongodb://mongo-secondary-2:27019/?directConnection=true",
    ]


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


SHARDED = {"sh4": 4, "sh7": 7, "sh8": 8}


class _NoProbing:
    """A client factory that fails the test if anything tries to connect."""

    def __init__(self, *args, **kwargs):
        raise AssertionError("a sharded deployment must not be probed for a primary")


def test_only_the_sharded_deployments_are_sharded():
    assert all(topology.is_sharded(name) for name in SHARDED)
    assert not any(topology.is_sharded(name) for name in ("single", "rs3", "rs5"))


@pytest.mark.parametrize("name", SHARDED)
def test_sharded_write_uri_names_only_the_router(name):
    # No replicaSet: the router is not a set member. No directConnection: the
    # driver must be free to discover it is talking to a router.
    uri = topology.write_uri(name)
    parsed = parse_uri(uri)
    assert parsed["nodelist"] == [("mongos", 27017)]
    assert "replicaset" not in parsed["options"]
    assert "directconnection" not in parsed["options"]
    assert topology_type_of(uri) != TOPOLOGY_TYPE.Single


@pytest.mark.parametrize("name", SHARDED)
def test_sharded_deployments_have_no_node_to_offload_reads_to(name):
    # Single-node shards have no secondary, so B1-style read offloading has
    # nowhere to send a read. [] is the honest answer, reached without probing.
    assert topology.secondary_uris(name, client_factory=_NoProbing) == []
    assert topology.all_node_uris(name) == []


def test_current_primary_is_refused_for_a_sharded_cluster():
    with pytest.raises(ValueError) as exc:
        topology.current_primary("sh8", client_factory=_NoProbing)
    assert "sharded" in str(exc.value)


@needs_docker
@pytest.mark.parametrize("name,shards", SHARDED.items())
def test_topology_map_agrees_with_the_stack_it_names(name, shards):
    """The map and the compose file describe one cluster twice. Drift between them
    sends the harness to an address the stack does not serve, or reports a shard
    count the stack does not have."""
    services = resolved(topology.compose_file(name), profiles=("run",))
    assert topology.shard_count(name) == shards
    assert sum("--shardsvr" in command(s) for s in services.values()) == shards
    host, port = parse_uri(topology.write_uri(name))["nodelist"][0]
    assert command(services[host]).startswith("mongos ")
    assert f"--port {port}" in command(services[host])
    assert services["runner"]["environment"]["MONGO_URI"] == topology.write_uri(name)


def test_sh1_is_sharded_with_one_shard():
    assert topology.is_sharded("sh1")
    assert topology.shard_count("sh1") == 1


def test_sh1_is_addressed_through_its_router():
    # Same code path as sh4 and sh8 with nothing to distribute across: it is the
    # intercept the scale axis is measured from. Comparing sh4 against a plain
    # standalone would change the router, the config server and the shard count
    # all at once.
    assert topology.write_uri("sh1") == "mongodb://mongos:27017"


def test_sh1_has_no_secondaries_to_read_from():
    assert topology.secondary_uris("sh1") == []


def test_sh7_is_a_seven_shard_deployment():
    # 7 shards + 1 control = 8 VMs = 8 external IPs, exactly the trial's
    # IN_USE_ADDRESSES cap, so the cloud sweep uses it in place of sh8.
    assert topology.is_sharded("sh7")
    assert topology.shard_count("sh7") == 7
    assert topology.compose_file("sh7") == "docker-compose.shard7.yml"
