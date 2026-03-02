"""Smoke-test for odbc-python (requires INFORMIX_CONNECTION_STRING env var)."""

import os
import sys

# Allow running from the repo root without installing.
sys.path.insert(0, os.path.dirname(__file__))

from connection import connect  # noqa: E402


def main() -> None:
    conn_str = os.environ.get("INFORMIX_CONNECTION_STRING")
    if not conn_str:
        print("Please set INFORMIX_CONNECTION_STRING environment variable.")
        print(
            "Example: DRIVER={IBM INFORMIX ODBC DRIVER};"
            "SERVER=ol_informix1210;DATABASE=testdb;UID=informix;PWD=password;"
        )
        return

    try:
        print(f"Connecting to {conn_str}...")
        with connect(conn_str) as conn:
            print(f"Connected! DBMS: {conn._dbms_name}")
            print(f"Informix? {conn._is_informix}")
            print(f"Ping: {conn.ping()}")

            with conn.cursor() as cur:
                query = "SELECT first 10 * FROM systables"
                print(f"Executing: {query}")
                cur.execute(query)

                if cur.description:
                    headers = [d[0] for d in cur.description]
                    print(f"Columns: {headers}")

                rows = cur.fetchall()
                print(f"Got {len(rows)} rows:")
                for row in rows:
                    print(row)

            # Transaction demo.
            print("\n--- Transaction test ---")
            with conn.begin() as tx:
                with conn.cursor() as cur:
                    cur.execute("SELECT first 1 * FROM systables")
                    print(f"In-tx row: {cur.fetchone()}")
                # tx auto-commits on clean exit.

        print("Connection closed.")

    except Exception as e:
        print(f"Error: {e}")
        raise


if __name__ == "__main__":
    main()
