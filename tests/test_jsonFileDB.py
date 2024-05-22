import unittest

# from aerovaldb.jsonfiledb import AerovalJsonFileDB
import aerovaldb


class TestJsonFileDB(unittest.TestCase):
    def test_plugins(self):
        engines = aerovaldb.list_engines()
        print(engines)
        self.assertGreaterEqual(len(engines), 1)

    def test_get(self):
        with aerovaldb.open("json_files:./test/testdb/json") as db:
            data = db._get(
                "/glob_stats",
                {"project": "project", "experiment": "experiment", "frequency": "frequency"},
            )
            self.assertEqual(data["path"], "./project/experiment/hm/")

    def test_put(self):
        with aerovaldb.open("json_files:./test/testdb/tmp") as db:
            obj = {"data": "gibberish"}
            data = db._put(
                obj,
                "/glob_stats",
                {"project": "test", "experiment": "test", "frequency": "monthly"},
            )

            read_data = db._get(
                "/glob_stats",
                {"project": "project", "experiment": "experiment", "frequency": "frequency"},
            )

            self.assertEqual(obj, read_data)
