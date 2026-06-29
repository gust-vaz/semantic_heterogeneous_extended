import ast
import json
from datetime import datetime


def parse_dict(text):
    """Accept the dataset's single-quoted dict syntax, then fall back to JSON."""
    try:
        return ast.literal_eval(text)
    except (ValueError, SyntaxError):
        return json.loads(text)


class Repl:
    def __init__(self, session, deployment=None):
        self.session = session
        self.deployment = deployment

    def handle(self, line):
        line = line.strip()
        if not line:
            return ""
        verb, _, rest = line.partition(" ")
        rest = rest.strip()

        if verb == "exit":
            return None
        if verb == "status":
            return "  ".join(f"{k}={v}" for k, v in self.session.status().items())
        if verb == "query":
            return "\n".join(str(r) for r in self.session.query(parse_dict(rest))) or "(no rows)"
        if verb == "count":
            return str(self.session.count(parse_dict(rest)))
        if verb == "insert":
            payload, _, date = rest.rpartition(" ")
            self.session.insert(payload.strip(), datetime.strptime(date.strip(), "%Y-%m-%d"))
            return "inserted 1"
        if verb == "load":
            parts = rest.split()
            folder = parts[0]
            date_field = parts[1] if len(parts) > 1 else "RefDate"
            self.session.load_csv(folder, date_field=date_field)
            return f"loaded {folder}"
        if verb == "operations":
            self.session.apply_operations_csv(rest)
            return f"applied operations from {rest}"
        if verb == "operation":
            op_type, date, args_text = rest.split(" ", 2)
            self.session.apply_operation(op_type, datetime.strptime(date, "%Y-%m-%d"),
                                         parse_dict(args_text))
            return f"applied {op_type}"
        if verb == "read-node":
            parts = rest.split()
            node = parts[0]
            mode = parts[1] if len(parts) > 1 else "split"
            self.session.set_read_node(node, mode)
            return f"reads now from {node} ({mode})"
        if verb in ("drop", "destroy"):
            if rest != "--yes":
                return f"refusing: re-run `{verb} --yes` to confirm"
            self.session.drop()
            if verb == "destroy" and self.deployment is not None:
                self.deployment.down(purge=True)
            return f"{verb} complete"
        return f"unknown command: {verb}"

    def run(self):
        while True:
            try:
                line = input("mellow> ")
            except (EOFError, KeyboardInterrupt):
                print()
                return
            out = self.handle(line)
            if out is None:
                return
            if out:
                print(out)
