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
