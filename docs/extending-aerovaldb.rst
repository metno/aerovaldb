Extending AerovalDB
===================

AerovalDB is designed to allow custom implementations to allow storage in interchangeable storage backends. This is done by writing a new class inheriting from :class:`aerovaldb.AerovalDB`.

Unless overridden, getters and setters will be routed to :meth:`aerovaldb.AerovalDB._get` and :meth:`aerovaldb.AerovalDB._put` respectively, so this is were the main read and write behaviour should be implemented. Direct overrides of an endpoint should only be used when non-standard behaviour is needed for a specific endpoint. :meth:`aerovaldb.AerovalDB._get` and :meth:`aerovaldb.AerovalDB._put` receive a route (denoting the asset type), as well as arguments based on which to get/put data and are a convenient way for using route-based lookup (for example, :class:`aerovaldb.jsondb.jsonfiledb.AerovalJsonFileDB` translates the route into a file path template, while :class:`aerovaldb.sqlitedb.sqlitedb.AerovalSqliteDB` translates the route into a SQL table name).

The following minimal example illustrates the principles of how one would implement these functions.

.. code-block:: python

    import aerovaldb
    from aerovaldb.utils.uri import build_uri, parse_uri

    class InMemoryAerovalDB(aerovaldb.AerovalDB):
        """
        Minimal example of _get and _put in an aerovaldb implementation.
        """
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
            # prints 'obj'

While lacking in certain niceties such as proper exception handling, persistent storage, and support for all access types, it serves as an illustration of how most functionality in an AerovalDB implementation is implemented.

In addition some functions must be overridden. This includes all methods that do not access json; that is, map_overlays and report_images; functions that list assets; functions for accessing by uri; as well as functions that require different behaviour for backwards compatibility reasons (eg. contours). To see which functions may require to be overriden, I recommend looking a :code:`src/aerovaldb/jsondb/jsonfiledb.py` and searching for the :code:`@override`` decorator.

Testing
-------
The tests in :code:`tests/test_aerovaldb.py` implements tests that all implementations of the AerovalDB interface need to be able to pass. This is to ensure that implementations are in fact interchangeable. It is recommended that you test your implementation against this test-suite as soon as possible.

These tests rely on a test database in the data storage format that is being tested. The canonical version of this database is the one found in :code:`tests/test-db/json`. This runs into a bootstrapping problem, as the test-db for a new format is most easily created by copying the canonical database, which requires a (somewhat) functional database implementation.

Here is the recommended way of bootstrapping testing for a new implementation:

* Implement :meth:`aerovaldb.AerovalDB.get_by_uri` and :meth:`aerovaldb.AerovalDB.put_by_uri` for your new implementation.
* Add your implementation to the :code:`tests/utils/test_copy.py`` test and verify that it passes.
* Use :meth:`aerovaldb.utils.copy.copy_db_contents` to make a version of the test database in your new storage format.
* Add your implementation to the :code:`tests/test_aerovaldb.py` tests. To do this, the following changes need to be made:
   
   * The :code:`tmpdb` fixture needs to be able to create a guaranteed empty, temporary db instance for your storage format.
   * The :code:`TESTDB_PARAMETRIZATION` needs to be extended with the resource string matching the test-db created above.
   * The :code:`IMPLEMENTATION_PARAMETRIZATION`` needs to include the identifier for you implementation, so that it matches the tmpdb identifier.

* Tweak until all tests are green.
