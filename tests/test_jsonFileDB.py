import unittest

from aerovaldb.jsonfiledb import AerovalJsonFileDB


class TestJsonFileDB(unittest.TestCase):
    def test_get(self):
        with AerovalJsonFileDB("basedir") as db:
            data = db.get_heatmap_timeseries("DE", "concno2")
            self.assertEqual(data, "dummy")
            obj = {"xxx": "123"}
            data = db.put_heatmap_timeseries(obj, "XX", "concno2")
