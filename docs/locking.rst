Locking
=============

To ensure consistent writes, aerovaldb provides a locking mechanism which can be used to coordinate writes between multiple instances of aerovaldb. The lock applies on the entire database, not per-file.

For :class:`AerovalJsonFileDB` the locking mechanism uses a folder of lock files (:code:`~/.aerovaldb/lock` by default) to coordinate the lock. It is important that the file system where the lock files are stored supports `fcntl <https://linux.die.net/man/2/fcntl>`.

By default locking is disabled as it may impact performance. To enable, set the environment variable :code:`AVDB_USE_LOCKING=1`.

Overriding the lock-file directory
----------------------------------

To override the lock file directory, set the environment variable `AVDB_LOCK_DIR`

Example
-----------

The following example illustrates how to use locking in practice:

.. code-block:: python

    import aerovaldb

    with aerovaldb.open('json_files:.') as db:
        with db.lock():
            data = db.get_by_uri('./file.json', default={"counter": 0})
            data["counter"] += 1
            db.put_by_uri(data, './file.json')

The above is the recommended approach to locking as it will automatically release the lock when done. However it is also possible to acquire and release the lock manually:

.. code-block:: python

    import aerovaldb

    with aerovaldb.open('json_files:.') as db:
        lck = db.lock()
        data = db.get_by_uri('./file.json', default={"counter": 0})
        data["counter"] += 1
        db.put_by_uri(data, './file.json')
        lck.release()

Limitations
------------

Locking uses so-called advisory locks, i.e.

- Locking will not work for multiple instances of aerovaldb which are configured with different locking directories.
- Locking will not prevent other programs that don't use aerovaldb to access the files from reading or writing the files.

