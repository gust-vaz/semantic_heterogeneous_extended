"""
Convenience entry-point for running the test suite.

Usage:
    python tests.py          # via this script
    python -m pytest -v      # directly with pytest (preferred)
"""
import sys
import pytest

if __name__ == "__main__":
    sys.exit(pytest.main([
        "semantic_heterogeneous_database/tests/",
        "-v",
        *sys.argv[1:],   # forward any extra args (e.g. -k, --tb=short)
    ]))
