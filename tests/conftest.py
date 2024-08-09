import os

os.environ["AVDB_USE_LOCKING"] = "1"

pytest_plugins = ("pytest_asyncio",)
