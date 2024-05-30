import pytest

import aerovaldb


def test_plugins():
    engines = aerovaldb.list_engines()
    print(engines)
    assert len(engines) >= 1
