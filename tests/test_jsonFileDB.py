import unittest

# from aerovaldb.jsonfiledb import AerovalJsonFileDB
import aerovaldb
from aerovaldb.jsonfiledb import AccessType
from parameterized import parameterized_class


class TestJsonFileDB(unittest.TestCase):
    """ """

    def test_plugins(self):
        engines = aerovaldb.list_engines()
        print(engines)
        self.assertGreaterEqual(len(engines), 1)

    def test_get_glob_stats(self):
        with aerovaldb.open(f"json_files:./tests/test-db/json") as db:
            data = db.get_glob_stats(
                "project", "experiment", "frequency", access_type=AccessType.OBJ
            )

            self.assertEqual(data["path"], "./project/experiment/hm/")

    def test_put_glob_stats(self):
        # TODO: These tests should ideally cleanup after themselves. For now
        # it is best to delete ./tests/test-db/tmp before running to verify
        # that they run as intended.
        with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
            obj = {"data": "gibberish"}
            db.put_glob_stats(obj, "test1", "test2", "test3")

            read_data = db.get_glob_stats("test1", "test2", "test3")

            self.assertEqual(obj["data"], read_data["data"])

    def test_get_contour(self):
        with aerovaldb.open(f"json_files:./tests/test-db/json") as db:
            data = db.get_contour("project", "experiment")

            self.assertEqual(data["path"], "./project/experiment/contour/")

    def test_put_contour(self):
        with aerovaldb.open("json_files:./tests/test-db/tmp") as db:
            obj = {"data": "gibberish"}
            db.put_contour(obj, "test1", "test2")

            read_data = db.get_contour("test1", "test2")

            self.assertEqual(obj["data"], read_data["data"])
