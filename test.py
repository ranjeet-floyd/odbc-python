"""Quick smoke-test launcher — delegates to pytest integration tests.

Usage:
    # Set your connection string:
    export ODBC_TEST_DSN="DRIVER={...};SERVER=...;DATABASE=...;"
    # OR for Informix:
    export INFORMIX_TEST_DSN="DRIVER={IBM INFORMIX ODBC DRIVER};SERVER=...;"

    # Run all integration tests:
    python test.py

    # Run only connection tests:
    python test.py -k TestConnection

    # Run Informix-specific tests:
    python test.py -k TestInformix
"""

import subprocess
import sys


def main() -> None:
    args = [
        sys.executable, "-m", "pytest",
        "tests/test_integration.py",
        "-v", "--tb=short",
    ] + sys.argv[1:]

    print(f"Running: {' '.join(args)}")
    sys.exit(subprocess.call(args))


if __name__ == "__main__":
    main()
