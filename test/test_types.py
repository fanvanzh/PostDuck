"""Type mapping between DuckDB and the Postgres wire protocol.

Each test sends a value of a specific DuckDB type and verifies that psycopg2
decodes it into the expected Python object, confirming that the advertised
PG OID in the RowDescription matches the data format.
"""

import datetime
import decimal

import pytest


def test_int_types(cur):
    cur.execute(
        "SELECT 1::SMALLINT, 2::INTEGER, 3::BIGINT, 4::TINYINT"
    )
    small, integer, big, tiny = cur.fetchone()
    assert (small, integer, big, tiny) == (1, 2, 3, 4)


def test_float_and_double(cur):
    cur.execute("SELECT 1.5::REAL, 2.5::DOUBLE")
    r, d = cur.fetchone()
    assert r == pytest.approx(1.5)
    assert d == pytest.approx(2.5)


def test_boolean(cur):
    cur.execute("SELECT TRUE, FALSE")
    assert cur.fetchone() == (True, False)


def test_text_and_varchar(cur):
    cur.execute("SELECT 'hello'::VARCHAR, 'world'::TEXT")
    a, b = cur.fetchone()
    assert a == "hello"
    assert b == "world"


def test_date(cur):
    cur.execute("SELECT DATE '2024-01-15'")
    (d,) = cur.fetchone()
    assert d == datetime.date(2024, 1, 15)


def test_timestamp(cur):
    cur.execute("SELECT TIMESTAMP '2024-01-15 12:34:56'")
    (ts,) = cur.fetchone()
    assert ts == datetime.datetime(2024, 1, 15, 12, 34, 56)


def test_decimal(cur):
    cur.execute("SELECT 123.45::DECIMAL(10,2)")
    (v,) = cur.fetchone()
    assert v == decimal.Decimal("123.45")


def test_nulls(cur):
    cur.execute(
        "SELECT NULL::INT, NULL::TEXT, NULL::DATE, NULL::DOUBLE, NULL::BOOLEAN"
    )
    row = cur.fetchone()
    assert row == (None, None, None, None, None)


def test_negative_numbers(cur):
    cur.execute("SELECT -42::INT, -1.5::DOUBLE, -9999999999::BIGINT")
    assert cur.fetchone() == (-42, -1.5, -9999999999)


def test_round_trip_mixed_types(cur, fresh_table):
    """Write mixed-type rows using parameterized INSERT, read them back."""
    # The generic fixture table only has INT/VARCHAR/DOUBLE; create a richer one.
    cur.execute(
        "CREATE TABLE IF NOT EXISTS all_types ("
        " i INT, b BIGINT, f DOUBLE, s VARCHAR,"
        " bo BOOLEAN, d DATE, ts TIMESTAMP"
        ")"
    )
    cur.execute("DELETE FROM all_types")
    cur.execute(
        "INSERT INTO all_types VALUES (%s,%s,%s,%s,%s,%s,%s)",
        (
            7,
            10 ** 12,
            3.14,
            "text",
            True,
            datetime.date(2024, 5, 1),
            datetime.datetime(2024, 5, 1, 10, 20, 30),
        ),
    )
    cur.execute("SELECT * FROM all_types")
    row = cur.fetchone()
    assert row == (
        7,
        10 ** 12,
        3.14,
        "text",
        True,
        datetime.date(2024, 5, 1),
        datetime.datetime(2024, 5, 1, 10, 20, 30),
    )
    cur.execute("DROP TABLE all_types")


def test_row_description_oids_are_postgres_compatible(cur):
    """psycopg2 relies on PG OIDs to pick the text-decoder. If we advertised
    DuckDB-specific or unknown OIDs the driver would just return strings.
    Verify that common types decode to their native Python equivalents."""

    cur.execute(
        "SELECT 1::INTEGER AS i, 3.14::DOUBLE AS d, 'x'::TEXT AS s,"
        " DATE '2024-01-02' AS dt"
    )
    row = cur.fetchone()
    assert isinstance(row[0], int)
    assert isinstance(row[1], float)
    assert isinstance(row[2], str)
    assert isinstance(row[3], datetime.date)
