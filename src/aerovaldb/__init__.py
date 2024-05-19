from importlib import metadata

__version__ = metadata.version(__package__)

from .aerovaldb import AerovalDB
