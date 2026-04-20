"""Extended Query protocol: Parse/Bind/Describe/Execute.

psycopg2 uses the extended protocol whenever the query has Python parameters
(``%s`` placeholders). These tests poke at parameter binding, NULL handling,
``executemany``, and server-side cursors-like workflows.
"""

import psycopg2
import pytest


def test_parameterized_scalar(cur):
    cur.execute("SELECT %s::int + %s::int", (10, 20))
    assert cur.fetchone() == (30,)


def test_parameterized_string(cur):
    cur.execute("SELECT %s::text || ' ' || %s::text", ("hello", "world"))
    assert cur.fetchone() == ("hello world",)


def test_parameter_null(cur):
    cur.execute("SELECT %s::int, %s::text", (None, None))
    assert cur.fetchone() == (None, None)


def test_insert_parameterized(cur, fresh_table):
    cur.execute(
        f"INSERT INTO {fresh_table} VALUES (%s, %s, %s)", (42, "answer", 3.14)
    )
    cur.execute(f"SELECT * FROM {fresh_table}")
    assert cur.fetchone() == (42, "answer", 3.14)


def test_executemany(cur, fresh_table):
    rows = [(i, f"r{i}", float(i) * 1.5) for i in range(1, 11)]
    cur.executemany(
        f"INSERT INTO {fresh_table} VALUES (%s, %s, %s)", rows
    )
    cur.execute(f"SELECT COUNT(*), SUM(id)::BIGINT FROM {fresh_table}")
    count, total = cur.fetchone()
    assert count == 10
    assert total == 55


def test_where_with_parameter(cur, fresh_table):
    cur.executemany(
        f"INSERT INTO {fresh_table} VALUES (%s, %s, %s)",
        [(i, f"r{i}", 0.0) for i in range(1, 6)],
    )
    cur.execute(f"SELECT id FROM {fresh_table} WHERE id > %s ORDER BY id", (2,))
    assert [r[0] for r in cur.fetchall()] == [3, 4, 5]


def test_fetchmany_incremental(cur):
    cur.execute("SELECT i FROM generate_series(1, 100) AS t(i)")
    batch1 = cur.fetchmany(25)
    batch2 = cur.fetchmany(25)
    batch3 = cur.fetchall()
    assert len(batch1) == 25
    assert len(batch2) == 25
    assert len(batch3) == 50
    assert batch1[0] == (1,)
    assert batch3[-1] == (100,)


def test_server_side_prepared_statement(conn):
    """psycopg2's ``PREPARE`` uses server-side prepared statements.

    DuckDB's ``PREPARE ... (int, int)`` syntax is unsupported, so we use the
    untyped form plus explicit casts inside the body.
    """
    with conn.cursor() as c:
        c.execute("PREPARE myq AS SELECT $1::int + $2::int")
        c.execute("EXECUTE myq(10, 20)")
        assert c.fetchone() == (30,)
        c.execute("DEALLOCATE myq")


def test_mixed_null_and_value_parameters(cur, fresh_table):
    cur.execute(
        f"INSERT INTO {fresh_table} VALUES (%s,%s,%s)", (1, None, 1.5)
    )
    cur.execute(f"SELECT id, name, v FROM {fresh_table}")
    assert cur.fetchone() == (1, None, 1.5)
