# content of test_sample.py
import unittest
import datetime
import pytest

from nikon_ETL import Base


def func(x):
    return x + 1


def test_answer():
    assert func(3) == 4


class TestFormat(unittest.TestCase, Base):

    def setUp(self):
        self.row = [{'apname': 'EDC_Import', 'last_end_time': datetime.datetime(
            2017, 10, 26, 23, 31, 27), 'virtual_recipe': None}]
        self.endtime_data = []

    def test_get_lastendtime(self):
        lastendtime = self.get_lastendtime(row=self.row)
        assert lastendtime == datetime.datetime.strptime(
            '2017-10-26 23:31:27', '%Y-%m-%d %H:%M:%S')

    def test_ckflow(self):
        assert self.check_flow(row=self.row) == True

    def test_column_state(self):
        pass

    def test_clean_edcdata(self):
        pass

    def test_clean_schemacolnames(self):
        pass


class TestDB(unittest.TestCase):
    """docstring for TestDB
    should init a mock db
    """
    
    def setUp(self):
        pass
