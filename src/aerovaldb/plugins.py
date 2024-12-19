import functools
import os
import sys
import warnings

if sys.version_info >= (3, 10):
    from importlib.metadata import EntryPoints, entry_points
else:
    from importlib_metadata import EntryPoints, entry_points

from .aerovaldb import AerovalDB


def _build_db_engines(entrypoints: EntryPoints) -> dict[str, AerovalDB]:
    backend_entrypoints: dict[str, AerovalDB] = {}
    for entrypoint in entrypoints:
        name = entrypoint.name
        if name in backend_entrypoints:
            warnings.warn(
                f"found multiple versions of {entrypoint.group} entrypoint {name} for {entrypoint.value}"
            )
            continue
        try:
            backend_entrypoints[name] = entrypoint.load()
        except Exception as ex:
            warnings.warn(f"Engine {name!r} loading failed:\n{ex}", RuntimeWarning)
    return backend_entrypoints


@functools.lru_cache(maxsize=1)
def list_engines() -> dict[str, AerovalDB]:
    """
    Return a dictionary of available AerovalDB classes.

    Returns
    -------
    dictionary

    Notes
    -----
    This function lives in the backends namespace (``dbs = aerovaldb.list_engines()``).

    """
    entrypoints = entry_points(group="aerovaldb")
    return _build_db_engines(entrypoints)


def open(resource, /, use_async: bool = False) -> AerovalDB:
    """Open an AerovalDB instance, sending args and kwargs
    directly to the `AervoalDB()` function

    :param resource: the resource-identifier for the database. The resource can be
        - 'entrypoint:path', with entrypoint being the type of database connection
        (eg. 'json_files' or 'sqlitedb') being the location where the database is
        located (eg. 'json_files:.')

        - 'path', a path to a json_files folder or sqlite file.

        - ':memory:' an sqlite in-memory database. Contents are not persistently
        stored!

    Examples

    >>> import aerovaldb
    >>> with aerovaldb.open(":memory:") as db:
    ...     db.put_experiments({'test': 'test'}, 'project')
    ...     db.get_experiments('project')
    {'test': 'test'}

    >>> import aerovaldb
    >>> db = aerovaldb.open(":memory:")
    >>> db.put_experiments({'test': 'test'}, 'project')
    >>> db.get_experiments('project')
    {'test': 'test'}
    """
    if resource == ":memory:":
        # Special case for sqlite in memory database.
        name = "sqlitedb"
        path = ":memory:"

    elif ":" in resource:
        parts = resource.split(":")
        if len(parts) > 1:
            name = parts[0]
            path = ":".join(parts[1:])
        else:
            # TODO check if path contains a aerovaldb cfg file
            name = "json_files"
            path = resource
    else:
        fileextension = os.path.splitext(resource)[1]
        if fileextension in [".db", ".sqlite"]:
            # Sqlite file.
            name = "sqlitedb"
            path = resource
        else:
            # Assume directory and json.
            name = "json_files"
            path = resource

    aerodb = list_engines()[name]

    return aerodb(path, use_async=use_async)  # type: ignore
