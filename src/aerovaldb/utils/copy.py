from .. import AerovalDB, open, AccessType
from ..utils import async_and_sync


@async_and_sync
async def copy_db_contents(source: str | AerovalDB, dest: str | AerovalDB):
    if isinstance(source, str):
        source = open(source)

    if isinstance(dest, str):
        dest = open(dest)

    if len(await dest.list_all()) > 0:
        ValueError("Destination database is not empty.")

    for uri in await source.list_all():
        data = await source.get_by_uri(uri, access_type=AccessType.JSON_STR)
        await dest.put_by_uri(data, uri)

    dst_len = len(await dest.list_all())
    src_len = len(await source.list_all())
    if dst_len != src_len:
        raise IOError(
            f"Unexpected number of items in destination after copy. Expected {src_len}, got {dst_len}"
        )
