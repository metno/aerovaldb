from importlib import metadata

__version__ = metadata.version(__package__)

from .aerovaldb import AerovalDB
from .exceptions import *
from .plugins import list_engines, open
from .types import *
