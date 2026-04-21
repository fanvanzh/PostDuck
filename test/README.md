# PostDuck Python test suite

Integration tests that start a real `postduck` binary and talk to it with
`psycopg2`, exercising the Postgres wire protocol end-to-end.

## Layout

- `conftest.py` – pytest fixtures: locates the `postduck` binary, boots it on a
  random free port with a temporary data directory, yields a ready-to-use
  `psycopg2` connection, and tears the server down after tests finish.
- `test_basic.py` – simple query protocol, DDL/DML, multi-statement queries.
- `test_extended.py` – extended query protocol (parameterized statements,
  `executemany`, server-side prepared statements, NULL handling).
- `test_types.py` – type mapping between DuckDB and Postgres OIDs / Python
  types (int, bigint, float, bool, text, date, timestamp, decimal).
- `test_transactions.py` – `BEGIN`/`COMMIT`/`ROLLBACK` behaviour, auto-commit,
  recovery from aborted transactions.
- `test_errors.py` – malformed SQL, catalog errors, error-response codes.

## Setup

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r test/requirements.txt
```

Build `postduck` once (see top-level README):

```bash
mkdir -p build && cd build && cmake .. && make -j
```

## Run

```bash
# run the whole suite
pytest -v test/

# run a single file
pytest -v test/test_basic.py

# point the tests at a custom binary or already-running server
POSTDUCK_BIN=/path/to/postduck pytest -v test/
POSTDUCK_URL=postgresql://user@127.0.0.1:5432/mydb pytest -v test/
```

By default `conftest.py` looks for the binary in these locations (first hit
wins):

1. `$POSTDUCK_BIN`
2. `<repo>/build/postduck`
3. `postduck` on `$PATH`

If `$POSTDUCK_URL` is set the fixtures skip spawning a server and reuse the
provided URL — useful when running the tests against a long-lived instance.
