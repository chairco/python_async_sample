# content of test_sample.py
import unittest
import datetime

from nikon_ETL import get_lastendtime, ckflow



def func(x):
    return x + 1


def test_answer():
    assert func(3) == 4


class TestFormat(unittest.TestCase):

    def setUp(self):
        self.row = [{'apname': 'EDC_Import', 'last_end_time': datetime.datetime(2017, 10, 26, 23, 31, 27), 'virtual_recipe': None}]
        self.endtime_data = []

    def test_get_lastendtime(self):
        #row = [{'apname': 'EDC_Import', 'last_end_time': datetime.datetime(2017, 10, 26, 23, 31, 27), 'virtual_recipe': None}]
        lastendtime = get_lastendtime(row=self.row)
        assert lastendtime == datetime.datetime.strptime('2017-10-26 23:31:27', '%Y-%m-%d %H:%M:%S')

    def test_ckflow(self):
        #row = [{'apname': 'EDC_Import', 'last_end_time': datetime.datetime(2017, 10, 26, 23, 31, 27), 'virtual_recipe': None}]
        assert ckflow(row=self.row) == True


