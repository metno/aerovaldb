from aerovaldb.utils.string_mapper.mapper import *


def test_initialization():
    mapper = StringMapper(
        {
            "a": "test",
            "b": ["test1", "test2"],
        },
        version_provider=None,
    )
    assert isinstance(mapper._lookuptable["a"], list) and all(
        [isinstance(x, ConstantMapper) for x in mapper._lookuptable["a"]]
    )

    assert (
        isinstance(mapper._lookuptable["b"], list)
        and len(mapper._lookuptable["b"]) == 1
        and isinstance(mapper._lookuptable["b"][0], PriorityMapper)
    )
