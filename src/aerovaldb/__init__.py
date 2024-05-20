from importlib import metadata

__version__ = metadata.version(__package__)

from .plugins import list_engines, open
