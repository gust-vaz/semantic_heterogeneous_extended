import argparse
import sys
from datetime import datetime
from mellow_cli.session import MellowSession
from mellow_cli.deployment import Deployment, node_uri
from mellow_cli.repl import Repl, parse_dict


def resolve_mongo_uri(deployment, mongo_uri, client_factory=None):
    if mongo_uri:
        return mongo_uri
    if deployment is None:
        raise SystemExit("error: nothing to connect to. Pass --deployment or --mongo-uri, "
                         "or run `up --deployment ...`.")
    dep = Deployment(deployment)
    if client_factory is None:
        return dep.primary_uri()
    return dep.primary_uri(client_factory=client_factory)


def _add_conn_args(p):
    p.add_argument("--deployment", choices=["single", "rs3", "rs5"])
    p.add_argument("--db")
    p.add_argument("--collection", default="records")
    p.add_argument("--mode", default="preprocess", choices=["preprocess", "rewrite"])
    p.add_argument("--write-concern", default="majority")
    p.add_argument("--mongo-uri")
    p.add_argument("--read-node", default=None)
    p.add_argument("--read-mode", default="split", choices=["split", "single_source"])


def build_parser():
    parser = argparse.ArgumentParser(prog="mellow_cli")
    sub = parser.add_subparsers(dest="command")

    for name in ("up", "shell", "connect"):
        sp = sub.add_parser(name)
        _add_conn_args(sp)

    for name in ("status", "drop", "destroy"):
        sp = sub.add_parser(name)
        _add_conn_args(sp)
        if name in ("drop", "destroy"):
            sp.add_argument("--yes", action="store_true")

    for name in ("query", "count"):
        sp = sub.add_parser(name)
        _add_conn_args(sp)
        sp.add_argument("query")

    sp = sub.add_parser("load")
    _add_conn_args(sp)
    sp.add_argument("folder")
    sp.add_argument("--date-field", default="RefDate")

    sp = sub.add_parser("operations")
    _add_conn_args(sp)
    sp.add_argument("file")

    sp = sub.add_parser("insert")
    _add_conn_args(sp)
    sp.add_argument("json")
    sp.add_argument("--valid-from", required=True)

    sp = sub.add_parser("operation")
    _add_conn_args(sp)
    sp.add_argument("op_type")
    sp.add_argument("valid_from")
    sp.add_argument("args")

    return parser


def _make_session(args):
    uri = resolve_mongo_uri(args.deployment, args.mongo_uri)
    read_uri = node_uri(args.read_node) if args.read_node else None
    return MellowSession(uri, args.db, collection=args.collection,
                         operation_mode=args.mode, write_concern=args.write_concern,
                         read_uri=read_uri, read_mode=args.read_mode)


def main(argv=None):
    args = build_parser().parse_args(argv)
    if args.command is None:
        print("error: nothing to connect to.\n"
              "  try: mellow connect --deployment rs3 --db mortality\n"
              "       mellow up --deployment rs3", file=sys.stderr)
        return 1

    if args.command == "up":
        Deployment(args.deployment).up()
        Repl(_make_session(args), Deployment(args.deployment)).run()
        return 0

    if args.command in ("shell", "connect"):
        Repl(_make_session(args), Deployment(args.deployment) if args.deployment else None).run()
        return 0

    session = _make_session(args)
    if args.command == "status":
        print("  ".join(f"{k}={v}" for k, v in session.status().items()))
    elif args.command == "query":
        for row in session.query(parse_dict(args.query)):
            print(row)
    elif args.command == "count":
        print(session.count(parse_dict(args.query)))
    elif args.command == "load":
        session.load_csv(args.folder, date_field=args.date_field)
        print("load complete")
    elif args.command == "operations":
        session.apply_operations_csv(args.file)
        print("operations applied")
    elif args.command == "insert":
        session.insert(args.json, datetime.strptime(args.valid_from, "%Y-%m-%d"))
        print("inserted 1")
    elif args.command == "operation":
        session.apply_operation(args.op_type,
                                datetime.strptime(args.valid_from, "%Y-%m-%d"),
                                parse_dict(args.args))
        print(f"applied {args.op_type}")
    elif args.command in ("drop", "destroy"):
        if not args.yes:
            print(f"refusing: re-run `{args.command} --yes` to confirm", file=sys.stderr)
            return 1
        session.drop()
        if args.command == "destroy" and args.deployment:
            Deployment(args.deployment).down(purge=True)
        print(f"{args.command} complete")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
