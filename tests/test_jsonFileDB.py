import unittest

# from aerovaldb.jsonfiledb import AerovalJsonFileDB
import aerovaldb


class TestJsonFileDB(unittest.TestCase):
    def test_plugins(self):
        engines = aerovaldb.list_engines()
        print(engines)
        self.assertGreaterEqual(len(engines), 1)

    def test_get(self):
        with aerovaldb.open("json_files:basedir") as db:
            data = db.get_heatmap_timeseries("DE", "concno2")
            self.assertEqual(data, "dummy")
            obj = {"xxx": "123"}
            data = db.put_heatmap_timeseries(obj, "XX", "concno2")
