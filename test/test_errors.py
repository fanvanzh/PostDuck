"""Error reporting: server produces well-formed ErrorResponse messages."""

import psycopg2
import pytest


def test_syntax_error(cur):
    with pytest.raises(psycopg2.Error) as exc:
        cur.execute("SELEKT 1")
    # psycopg2 wraps the server error; message should mention the server text.
    assert "syntax" in str(exc.value).lower() or "parser" in str(exc.value).lower()


def test_unknown_table(cur):
    with pytest.raises(psycopg2.Error) as exc:
        cur.execute("SELECT * FROM definitely_no_such_table")
    assert "catalog" in str(exc.value).lower() or "does not exist" in str(exc.value).lower()


def test_type_error_in_query(cur):
    with pytest.raises(psycopg2.Error):
        cur.execute("SELECT 'abc'::INT")


def test_error_then_new_query_same_connection(postduck_server):
    """Session must recover after a user error so the next query succeeds."""
    c = postduck_server.connect()
    c.autocommit = True
    try:
        with c.cursor() as cur:
            with pytest.raises(psycopg2.Error):
                cur.execute("SELECT foo_bar_baz")
            # Same connection, brand-new statement should still work.
            cur.execute("SELECT 1")
            assert cur.fetchone() == (1,)
    finally:
        c.close()


def test_conversion_error_is_reported(cur):
    """DuckDB returns +inf for 1/0 (IEEE-754), so test a conversion error
    instead — this exercises the same ErrorResponse path."""
    with pytest.raises(psycopg2.Error):
        cur.execute("SELECT CAST('not-a-number' AS INTEGER)")
