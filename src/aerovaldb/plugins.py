import functools
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
    """open an AerovalDB directly, sending args and kwargs
    directly to the `AervoalDB()` function

    :param resource: the resource-name for the database. The resource can be
        - 'entrypoint:path', with path being the location where the database should be generated
        (eg. 'json_files:.')
        - 'path', with path containing either an aerovaldb.cfg (Not yet implemented) configuration
        or path being a json_files dabasase (for example, '.' is equivalent to 'json_files:.')
    :param use_async : If true, aiofile will be used to read files, otherwise files will be read
        synchronously.
    :return: an implementation-object of AerovalDB openend to a location
    """

    if ":" in resource:
        parts = resource.split(":")
        if len(parts) > 1:
            name = parts[0]
            path = ":".join(parts[1:])
        else:
            # TODO check if path contains a aerovaldb cfg file
            name = "json_files"
            path = resource
    else:
        # Assume directory and json.
        # TODO: In the future this should differentiate based on file path, eg. folder->json_files
        # .sqlite-> SqliteDB, etc.
        name = "json_files"
        path = resource

    aerodb = list_engines()[name]

    return aerodb(path, use_async=use_async)  # type: ignore
