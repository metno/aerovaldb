import aiofile
import time
import random
import os
import aiofile
import asyncio


def generate_test_file(fp: str, /, size: int):
    with open(fp, "w") as f:
        for i in range(size):
            bytes = random.randbytes(1024)
            f.write(bytes.hex())


def generate_test_files():
    if not os.path.exists("test-file-1kb"):
        print("Generate 1kb test file")
        generate_test_file("test-file-1kb", 1)

    if not os.path.exists("test-file-100kb"):
        print("Generate 100kb test file")
        generate_test_file("test-file-100kb", 100)

    if not os.path.exists("test-file-10000kb"):
        print("Generate 10000kb test file")
        generate_test_file("test-file-10000kb", 100000)

    if not os.path.exists("test-file-250000kb"):
        print("Generate 250000kb test file")
        generate_test_file("test-file-250000kb", 250000)


generate_test_files()

for fp in (
    "test-file-1kb",
    "test-file-100kb",
    "test-file-10000kb",
    "test-file-250000kb",
):
    print(f"Testing file {fp}")
    print(f" Testing synchronous read")
    start_time = time.perf_counter()
    for _ in range(10):
        with open(fp, "r") as f:
            f.read()
    print(f" Time elapsed: {time.perf_counter()-start_time:.2f}s")

    async def async_read(fpath: str):
        for _ in range(10):
            async with aiofile.async_open(fpath) as f:
                await f.read()

    print(f" Testing asynchronous read")
    start_time = time.perf_counter()
    for _ in range(10):
        asyncio.run(async_read(fp))
    print(f" Time elapsed: {time.perf_counter()-start_time:.2f}s")
