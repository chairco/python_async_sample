# content of test_db.py
import unittest
import pytest

from itertools import chain

from dbs import db_pg, nikon


def test_psql_connect():
    cursor = db_pg.get_cursor()
    assert str(type(cursor)) == "<class 'psycopg2.extensions.cursor'>"


@pytest.fixture
def get_toolids():
    """connect db and get tlcd's rawdata information
    """
    cursor = db_pg.get_cursor()
    cursor.execute(
        """
        SELECT 'relname' 
        FROM pg_class t
        WHERE 1=1
        AND t.relname LIKE 'tlcd%_rawdata'
        """
    )
    rows = cursor.fetchall()
    return list(chain.from_iterable(rows))


def test_tlcdamout(get_toolids):
    assert len(get_toolids) == 25


class TestETL(unittest.TestCase):

    def setUp(self):
        cursor = db_pg.get_cursor()
        cursor.execute(
            """
            SELECT 'relname' 
            FROM pg_class t
            WHERE 1=1
            AND t.relname LIKE 'tlcd%_rawdata'
            """
        )
        rows = cursor.fetchall()
        self.toolids = list(chain.from_iterable(rows))

    def test_tlcdamout(self):
        assert len(self.toolids) == 25
