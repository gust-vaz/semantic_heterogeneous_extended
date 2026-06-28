"""Small, dependency-free helpers shared by the benchmark scripts."""


def effective_write_concern(token, nodes):
    """Translate a CLI write-concern token into a value PyMongo's WriteConcern accepts.

    MongoDB has no literal w="all"; "all nodes" must be the integer member count.
    'majority' is passed through unchanged. Digit strings/ints become ints.
    """
    if token == "all":
        return int(nodes)
    if isinstance(token, int):
        return token
    if str(token).isdigit():
        return int(token)
    return token  # e.g. 'majority'
