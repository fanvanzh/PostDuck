"""Shared pytest fixtures for the PostDuck integration test suite.

The main fixtures spin up a real ``postduck`` binary on a random free port,
backed by a temporary data directory, and hand out ``psycopg2`` connections to
the tests. At teardown the server is stopped and the temporary directory is
removed.

Environment variables
---------------------
POSTDUCK_BIN
    Absolute path to the ``postduck`` executable. If unset the fixtures search
    ``<repo>/build/postduck`` first, then fall back to ``$PATH``.

POSTDUCK_URL
    If set, tests reuse this URL instead of spawning a server. Useful when a
    long-lived instance is already running.
"""

from __future__ import annotations

import os
import pathlib
import shutil
import signal
import socket
import subprocess
import tempfile
import time
from typing import Iterator, Optional

import psycopg2
import pytest


ROOT_DIR = pathlib.Path(__file__).resolve().parent.parent
STARTUP_TIMEOUT_SECS = 10.0


def _find_postduck_binary() -> pathlib.Path:
    env_bin = os.environ.get("POSTDUCK_BIN")
    if env_bin:
        path = pathlib.Path(env_bin)
        if path.is_file() and os.access(path, os.X_OK):
            return path
        raise FileNotFoundError(f"POSTDUCK_BIN={env_bin!r} is not an executable file")

    candidates = [
        ROOT_DIR / "build" / "postduck",
        ROOT_DIR / "postduck",
    ]
    for path in candidates:
        if path.is_file() and os.access(path, os.X_OK):
            return path

    on_path = shutil.which("postduck")
    if on_path:
        return pathlib.Path(on_path)

    raise FileNotFoundError(
        "Could not locate the 'postduck' binary. "
        "Set POSTDUCK_BIN, or build it into <repo>/build/postduck."
    )


def _free_tcp_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def _wait_for_port(host: str, port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_err: Optional[Exception] = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=0.5):
                return
        except OSError as exc:
            last_err = exc
            time.sleep(0.05)
    raise TimeoutError(
        f"postduck did not start listening on {host}:{port} within "
        f"{timeout:.1f}s ({last_err})"
    )


class PostduckServer:
    """Handle to a running PostDuck server process."""

    def __init__(self, host: str, port: int, dbname: str, user: str,
                 data_dir: pathlib.Path, proc: Optional[subprocess.Popen]):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.data_dir = data_dir
        self.proc = proc

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password=x"
        )

    def connect(self, **extra) -> psycopg2.extensions.connection:
        return psycopg2.connect(self.dsn, **extra)

    def stop(self) -> None:
        if self.proc is None:
            return
        if self.proc.poll() is None:
            self.proc.send_signal(signal.SIGINT)
            try:
                self.proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait()


@pytest.fixture(scope="session")
def postduck_server() -> Iterator[PostduckServer]:
    """Session-scoped fixture that starts exactly one postduck server."""

    external_url = os.environ.get("POSTDUCK_URL")
    if external_url:
        # Parse out host/port/dbname for diagnostics; reuse the URL as DSN.
        parsed = psycopg2.extensions.parse_dsn(external_url)
        yield PostduckServer(
            host=parsed.get("host", "127.0.0.1"),
            port=int(parsed.get("port", 5432)),
            dbname=parsed.get("dbname", "postduck"),
            user=parsed.get("user", "postduck"),
            data_dir=pathlib.Path("."),
            proc=None,
        )
        return

    binary = _find_postduck_binary()
    port = _free_tcp_port()
    data_dir = pathlib.Path(tempfile.mkdtemp(prefix="postduck-test-"))

    log_path = data_dir / "postduck.log"
    log_fh = open(log_path, "wb")
    proc = subprocess.Popen(
        [
            str(binary),
            "--port", str(port),
            "--data", str(data_dir),
            "--thread", "4",
            "--log", "INFO",
        ],
        stdout=log_fh,
        stderr=subprocess.STDOUT,
        # Put the server in its own process group so SIGINT only hits the server.
        start_new_session=True,
    )

    try:
        _wait_for_port("127.0.0.1", port, STARTUP_TIMEOUT_SECS)
    except Exception:
        proc.kill()
        proc.wait()
        log_fh.close()
        shutil.rmtree(data_dir, ignore_errors=True)
        raise

    server = PostduckServer(
        host="127.0.0.1",
        port=port,
        dbname="postduck_test",
        user="postduck",
        data_dir=data_dir,
        proc=proc,
    )

    try:
        yield server
    finally:
        server.stop()
        log_fh.close()
        # Keep the log on failure for post-mortem; always clean data files.
        # shutil.rmtree will remove the log too, which is fine in CI.
        shutil.rmtree(data_dir, ignore_errors=True)


@pytest.fixture()
def conn(postduck_server: PostduckServer) -> Iterator[psycopg2.extensions.connection]:
    """Fresh autocommit connection, closed at the end of each test."""
    c = postduck_server.connect()
    c.autocommit = True
    try:
        yield c
    finally:
        try:
            c.close()
        except Exception:
            pass


@pytest.fixture()
def cur(conn) -> Iterator[psycopg2.extensions.cursor]:
    """Convenience cursor on top of ``conn``."""
    cursor = conn.cursor()
    try:
        yield cursor
    finally:
        cursor.close()


@pytest.fixture()
def fresh_table(cur):
    """Create and drop a throwaway table, yielding its name.

    Tests that need a scratch table can depend on this fixture to get a
    clean one that's automatically cleaned up.
    """
    name = f"t_{os.urandom(4).hex()}"
    cur.execute(f"CREATE TABLE {name} (id INTEGER, name VARCHAR, v DOUBLE)")
    try:
        yield name
    finally:
        try:
            cur.execute(f"DROP TABLE IF EXISTS {name}")
        except Exception:
            pass
