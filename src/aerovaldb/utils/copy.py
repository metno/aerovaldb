import argparse
import logging

from .. import AccessType, AerovalDB, open
from ..utils import async_and_sync

logger = logging.getLogger(__name__)


@async_and_sync
async def copy_db_contents(source: str | AerovalDB, dest: str | AerovalDB):
    """
    Utility function for copying the contents of one db connection to another
    Currently this implementation requires the destination db to be empty.

    :param source : Instance of AerovalDB or resource string passed to
        aerovaldb.open()
    :param dest : Instance of AerovalDB or resource string passed to
        aerovaldb.open()

    :raises : ValueError
        If destination is not empty.
    :raises : IOError
        Number of items in destination is not the same as the source after copy.
    """
    if isinstance(source, str):
        source = open(source)

    if isinstance(dest, str):
        dest = open(dest)

    if len(await dest.list_all()) > 0:
        ValueError("Destination database is not empty.")

    for i, uri in enumerate(await source.list_all()):
        logger.info(f"Processing item {i} of {len(await source.list_all())}")
        access = AccessType.JSON_STR
        if uri.startswith("/v0/report-image/") or uri.startswith("/v0/map-overlay/"):
            access = AccessType.BLOB
        data = await source.get_by_uri(uri, access_type=access)

        await dest.put_by_uri(data, uri)

    dst_len = len(await dest.list_all())
    src_len = len(await source.list_all())
    if dst_len != src_len:
        raise IOError(
            f"Unexpected number of items in destination after copy. Expected {src_len}, got {dst_len}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Utility for copying contents from one aerovaldb resource to another."
    )
    parser.add_argument(
        "source", type=str, help="Resource string of the source AerovalDB instance"
    )
    parser.add_argument(
        "dest", type=str, help="Resource string of the destination AerovalDB instance"
    )

    args = parser.parse_args()

    source = args.source
    dest = args.dest
    copy_db_contents(source, dest)


if __name__ == "__main__":
    main()
