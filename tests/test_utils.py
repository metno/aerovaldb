from aerovaldb.utils import parse_formatted_string, parse_uri
import pytest
from aerovaldb.routes import *


@pytest.mark.parametrize(
    "template,s,expected",
    (
        ("{test}", "hello", {"test": "hello"}),
        ("ABCD{test}1234", "ABCDhello world1234", {"test": "hello world"}),
        (
            "test/{a}/{b}/{c}/{d}",
            "test/1/2/3/4",
            {"a": "1", "b": "2", "c": "3", "d": "4"},
        ),
    ),
)
def test_parse_formatted_string(template: str, s: str, expected: dict):
    assert parse_formatted_string(template, s) == expected


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
