from enum import Enum, auto


class AccessType(Enum):
    """Enumeration of access types. Specifies how data will be read
    and returned.

    JSON_STR: Result will be returned as an unparsed json string.
    FILE_PATH: Result will be returned as the file path to the file
    containing the data.
    OBJ: The json will be parsed and returned as a python object.
    """

    JSON_STR = auto()
    FILE_PATH = auto()
    OBJ = auto()
