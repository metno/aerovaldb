Extending AerovalDB
===================

AerovalDB is designed to allow custom implementations to allow storage in interchangeable storage backends. This is done by writing a new class inheriting from :class:`aerovaldb.AerovalDB`.

Unless overridden, getters and setters will be routed to :meth:`aerovaldb.AerovalDB._get` and :meth:`aerovaldb.AerovalDB._put` respectively, so this is were the main read and write behaviour should be implemented. These functions receive a route, as well as arguments based on which to get/put data and are intended to be used based on route-lookup (for example, :class:`aerovaldb.jsondb.jsonfiledb.AerovalJsonFileDB` translates the route into a file path template, while :class:`aerovaldb.sqlitedb.sqlitedb.AerovalSqliteDB` translates the route into a table name).

The following minimal example illustrates the principles of how one would implement these functions.

.. code-block:: python

    import aerovaldb
    from aerovaldb.utils.uri import build_uri, parse_uri

    class InMemoryAerovalDB(aerovaldb.AerovalDB):
        def __init__(self):
            self._store = {}

        async def _get(self, route: str, route_args: dict, **kwargs):
            access_type = self._normalize_access_type(kwargs.pop("access_type", aerovaldb.AccessType.OBJ))

            if access_type != aerovaldb.AccessType.OBJ:
                raise ValueError(f"Unsupported accesstype, '{access_type}'.")

            # Alternatively you can use route as a lookup table.
            uri = build_uri(route, route_args, kwargs)
            return self._store[uri]

        async def _put(self, obj, route: str, route_args: dict, **kwargs):
            uri = build_uri(route, route_args, kwargs)

            self._store[uri] = obj


    if __name__ == "__main__":
        with InMemoryAerovalDB() as db:
            db.put_experiments("obj", "test")
            print(db.get_experiments("test"))

This is a minimal implementation and thus lacks certain niceties such as proper exception handling, persistent storage, and support for all access modes, but it serves to illustrate how most functionality in an AerovalDB implementation is implemented.

In addition some functions must be overridden. This includes all methods that do not access json; that is, map_overlays, report_images; functions that list assets; functions for accessing by uri; as well as functions that require different behaviour for backwards compatibility reasons (eg. contours). To see which functions require to be overriden, I recommend looking a jsonfiledb.py and searching for the '@override' decorator.

Testing
-------
The tests in tests/test_aerovaldb.py implements tests that all implementations of the AerovalDB interface need to be able to pass. This is to ensure that implementations are in fact interchangeable. It is recommended that you test your implementation against this as soon as possible.

These tests rely on a test database in the data storage format that is being tested. The canonical version of this database is the one found in tests/test-db/json. This runs into a bootstrapping problem, as the test-db for a new format is most easily created by copying this db, which requires a (somewhat) functional database implementation.

Here is the recommended way of doing this:

- Implement get_by_uri and put_by_uri for your new implementation.
- Add your implementation to the tests/utils/test_copy test and verify that it passes.
- Use the built-in copy function to make a version of the test database in your new storage format.
- Add your implementation to the test_aerovaldb tests. For this the following changes need to be made:
  - The tmpdb fixture needs to be able to create a guaranteed empty, temporary db instance for your storage format.
  - The TESTDB_PARAMETRIZATION needs to be extended with the resource string matching the test-db created above.
  - The IMPLEMENTATION_PARAMETRIZATION needs to include the identifier for you implementation, so that it matches the tmpdb identifier.
- `Tweak until all tests are green <https://img.ifunny.co/images/ada63efed0355a1c17aa761d6fdaa6d03ae7862ddccd0c75d1d0ff961c69deb0_1.jpg>``




