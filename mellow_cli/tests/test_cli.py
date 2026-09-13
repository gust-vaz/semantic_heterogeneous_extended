from mellow_cli.__main__ import build_parser, resolve_mongo_uri


def test_parser_reads_query_subcommand():
    args = build_parser().parse_args(
        ["query", "--deployment", "rs3", "--db", "mortality", "{'cid': 'X'}"])
    assert args.command == "query"
    assert args.deployment == "rs3"
    assert args.db == "mortality"
    assert args.query == "{'cid': 'X'}"


def test_bare_invocation_has_no_command():
    args = build_parser().parse_args([])
    assert args.command is None


def test_resolve_mongo_uri_prefers_explicit_override():
    assert resolve_mongo_uri(deployment=None, mongo_uri="mongodb://x:1") == "mongodb://x:1"


def test_resolve_mongo_uri_single_default():
    assert resolve_mongo_uri(deployment="single", mongo_uri=None) == \
        "mongodb://localhost:27017/?directConnection=true"


def test_main_up_without_deployment_errors_cleanly(capsys):
    from mellow_cli.__main__ import main
    rc = main(["up"])
    assert rc == 1
    assert "deployment" in capsys.readouterr().err.lower()


def test_parser_accepts_the_sharded_deployments():
    for name in ("sh4", "sh8"):
        assert build_parser().parse_args(["up", "--deployment", name]).deployment == name


def test_parser_accepts_every_deployment_the_cli_can_bring_up():
    # The parser kept its own hard-coded list, so a deployment added to the map
    # was still refused at the command line.
    from mellow_cli.deployment import DEPLOYMENTS
    for name in DEPLOYMENTS:
        assert build_parser().parse_args(["connect", "--deployment", name]).deployment == name


def test_up_without_deployment_lists_every_choice(capsys):
    from mellow_cli.__main__ import main
    assert main(["up"]) == 1
    err = capsys.readouterr().err
    assert all(name in err for name in ("single", "rs3", "rs5", "sh4", "sh8")), err
