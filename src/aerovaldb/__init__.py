from importlib import metadata

__version__ = metadata.version(__package__)

from .exceptions import *
from .plugins import list_engines, open
from .types import *
from .aerovaldb import AerovalDB
