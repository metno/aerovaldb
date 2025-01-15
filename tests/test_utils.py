import pytest

from aerovaldb.routes import *
from aerovaldb.utils import (
    decode_arg,
    encode_arg,
    extract_substitutions,
    parse_formatted_string,
    parse_uri,
)


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


@pytest.mark.parametrize(
    "template,string,expected",
    (
        ("{test}", "hello", {"test": "hello"}),
        ("ABCD{test}1234", "ABCDhelloworld1234", {"test": "helloworld"}),
        (
            "test/{a}/{b}/{c}/{d}",
            "test/A/B/C/D",
            {"a": "A", "b": "B", "c": "C", "d": "D"},
        ),
    ),
)
def test_parse_formatted_string(template: str, string: str, expected: dict):
    assert parse_formatted_string(template, string) == expected


@pytest.mark.parametrize(
    "template,string,val,exception",
    (
        ("{a}{b}", "abcd", "can not be disambiguated", Exception),
        (
            "{a}b{b}c",
            "testbhellotestblah",
            "did not match template string",
            Exception,
        ),
    ),
)
def test_parse_fromatted_string_error(template: str, string: str, val: str, exception):
    with pytest.raises(exception) as e:
        parse_formatted_string(template, string)

    assert val in str(e.value)


@pytest.mark.parametrize(
    "uri,expected",
    (
        (
            "/v0/experiments/project",
            (ROUTE_EXPERIMENTS, {"project": "project"}, {}),
        ),
        (
            "/v0/map/project/experiment/network/obsvar/layer/model/modvar?time=time",
            (
                ROUTE_MAP,
                {
                    "project": "project",
                    "experiment": "experiment",
                    "network": "network",
                    "obsvar": "obsvar",
                    "layer": "layer",
                    "model": "model",
                    "modvar": "modvar",
                },
                {"time": "time"},
            ),
        ),
    ),
)
def test_parse_uri(uri: str, expected: tuple[str, dict, dict]):
    template, route_args, kwargs = parse_uri(uri)

    assert (template, route_args, kwargs) == expected


def test_parse_uri_error():
    with pytest.raises(ValueError):
        parse_uri("??")


@pytest.mark.parametrize(
    "input,expected",
    (
        ("", ""),
        ("%", "%0"),
        ("/", "%1"),
        ("hello-world/hello%1234", "hello-world%1hello%01234"),
        ("%/" * 5, "%0%1" * 5),
    ),
)
def test_encode_decode_arg(input: str, expected: str):
    encoded = encode_arg(input)

    assert encoded == expected

    decoded = decode_arg(encoded)

    assert decoded == input
