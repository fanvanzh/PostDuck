"""Transaction handling: autocommit, explicit BEGIN/COMMIT/ROLLBACK, recovery."""

import psycopg2
import pytest


def test_autocommit_select(conn):
    conn.autocommit = True
    with conn.cursor() as c:
        c.execute("SELECT 1")
        assert c.fetchone() == (1,)


def test_non_autocommit_commit(postduck_server):
    """A committed change must be visible in a brand-new connection."""
    c1 = postduck_server.connect()
    try:
        with c1.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS txn_commit (v INT)")
            cur.execute("DELETE FROM txn_commit")
        c1.commit()

        # psycopg2 opens a transaction on first query when autocommit=False.
        c1.autocommit = False
        with c1.cursor() as cur:
            cur.execute("INSERT INTO txn_commit VALUES (1)")
        c1.commit()

        c2 = postduck_server.connect()
        try:
            with c2.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM txn_commit")
                assert cur.fetchone()[0] == 1
        finally:
            c2.close()
    finally:
        c1.close()


def test_rollback_discards_changes(postduck_server):
    c1 = postduck_server.connect()
    try:
        with c1.cursor() as cur:
            cur.execute("CREATE TABLE IF NOT EXISTS txn_rollback (v INT)")
            cur.execute("DELETE FROM txn_rollback")
        c1.commit()

        c1.autocommit = False
        with c1.cursor() as cur:
            cur.execute("INSERT INTO txn_rollback VALUES (42)")
        c1.rollback()

        c1.autocommit = True
        with c1.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM txn_rollback")
            assert cur.fetchone()[0] == 0
    finally:
        c1.close()


def test_aborted_transaction_auto_recovers(postduck_server):
    """After a failed statement, psycopg2 requires a ROLLBACK before it will
    run more queries. Make sure the server cleanly handles that sequence and
    that subsequent queries work against the same session."""
    c = postduck_server.connect()
    try:
        c.autocommit = False
        with c.cursor() as cur:
            cur.execute("SELECT 1")  # opens txn
            with pytest.raises(psycopg2.Error):
                cur.execute("SELECT * FROM no_such_table_12345")
        c.rollback()
        # Same connection must still be usable after rollback.
        with c.cursor() as cur:
            cur.execute("SELECT 2")
            assert cur.fetchone() == (2,)
    finally:
        c.close()


def test_set_unknown_guc_is_noop(cur):
    # JDBC sends SET extra_float_digits=3 on connect; we accept it.
    cur.execute("SET extra_float_digits = 3")
    # Still be able to run queries afterwards.
    cur.execute("SELECT 1")
    assert cur.fetchone() == (1,)
