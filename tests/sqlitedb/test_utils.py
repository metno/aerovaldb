import pytest
from aerovaldb.sqlitedb.utils import extract_substitutions


@pytest.mark.parametrize(
    "template,result",
    (
        ("{A}{B}{C}", {"A", "B", "C"}),
        ("{A}hello world{B} test {C}", {"A", "B", "C"}),
        ("", set()),
    ),
)
def test_extract_substitutions(template: str, result: set[str]):
    l = extract_substitutions(template)

    assert set(l) == result
