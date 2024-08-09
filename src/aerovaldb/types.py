from enum import Enum, auto


class AccessType(Enum):
    """Enumeration of access types. Specifies how data will be read
    and returned.

    JSON_STR: Result will be returned as an unparsed json string.
    FILE_PATH: Result will be returned as the file path to the file
    containing the data.
    OBJ: The json will be parsed and returned as a python object.
    URI: A string which is a unique identifier of this asset between
    implementations of Aerovaldb. Can be used with `get_by_uuid()` and
    `put_by_uuid()` to read or write respectively.
    """

    JSON_STR = auto()
    FILE_PATH = auto()
    OBJ = auto()
    URI = auto()
