"""Simple Query protocol basics: SELECT, DML, DDL, multi-statement."""

import psycopg2
import pytest


def test_select_constants(cur):
    cur.execute("SELECT 1 AS a, 'hello' AS b, 3.14::double AS c")
    row = cur.fetchone()
    assert row == (1, "hello", 3.14)


def test_column_names(cur):
    cur.execute("SELECT 1 AS alpha, 2 AS beta")
    cols = [d.name for d in cur.description]
    assert cols == ["alpha", "beta"]


def test_create_insert_select(cur, fresh_table):
    cur.execute(
        f"INSERT INTO {fresh_table} VALUES (1, 'foo', 1.5), (2, 'bar', 2.5)"
    )
    assert cur.rowcount == 2

    cur.execute(f"SELECT id, name, v FROM {fresh_table} ORDER BY id")
    assert cur.fetchall() == [(1, "foo", 1.5), (2, "bar", 2.5)]


def test_update_delete_counts(cur, fresh_table):
    cur.execute(
        f"INSERT INTO {fresh_table} VALUES (1,'a',1.0),(2,'b',2.0),(3,'c',3.0)"
    )
    cur.execute(f"UPDATE {fresh_table} SET name='z' WHERE id >= 2")
    assert cur.rowcount == 2

    cur.execute(f"DELETE FROM {fresh_table} WHERE id = 1")
    assert cur.rowcount == 1

    cur.execute(f"SELECT COUNT(*) FROM {fresh_table}")
    (count,) = cur.fetchone()
    assert count == 2


def test_multi_statement_simple_query(conn):
    """A single `Q` message with multiple ';'-separated statements should run
    them all and the cursor should reflect the last one's result."""
    with conn.cursor() as c:
        c.execute(
            "CREATE TABLE IF NOT EXISTS multi_stmt (x INT);"
            " DELETE FROM multi_stmt;"
            " INSERT INTO multi_stmt VALUES (1),(2),(3);"
            " SELECT SUM(x)::BIGINT FROM multi_stmt;"
        )
        (total,) = c.fetchone()
        assert total == 6
        c.execute("DROP TABLE multi_stmt")


def test_large_result_set(cur):
    cur.execute("SELECT i FROM generate_series(1, 5000) AS t(i)")
    rows = cur.fetchall()
    assert len(rows) == 5000
    assert rows[0] == (1,)
    assert rows[-1] == (5000,)


def test_empty_query_is_accepted(cur):
    # DB-API "empty" query; psycopg2 sends "; " through simple query.
    cur.execute(";")
    # pyscopg2 returns no rows; just making sure the connection survives
    cur.execute("SELECT 42")
    assert cur.fetchone() == (42,)


def test_server_version_parameter(conn):
    assert conn.get_parameter_status("server_version") is not None
    assert conn.get_parameter_status("server_encoding") == "UTF8"
    assert conn.get_parameter_status("client_encoding") == "UTF8"


def test_backend_pid_and_cancel(conn):
    """BackendKeyData is sent at startup; psycopg2 exposes it via get_backend_pid."""
    pid = conn.get_backend_pid()
    assert pid > 0
