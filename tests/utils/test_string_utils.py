import pytest

from aerovaldb.utils import str_to_bool


@pytest.mark.parametrize(
    "value,expected",
    (
        ("1", True),
        ("0", False),
        ("TrUe", True),
        ("FaLsE", False),
        ("T", True),
        ("F", False),
        ("YeS", True),
        ("No", False),
        ("Y", True),
        ("F", False),
    ),
)
def test_str_to_bool(value: str, expected: bool):
    assert str_to_bool(value) == expected


def test_str_to_bool_exception_1():
    with pytest.raises(ValueError):
        str_to_bool(None)


def test_str_to_bool_exception_2():
    with pytest.raises(ValueError):
        str_to_bool("blah")


def test_str_to_bool_default():
    assert str_to_bool("blah", default=True)
    assert not str_to_bool("blah", default=False)
