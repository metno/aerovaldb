import sys

try:
    import numpy as np
except ImportError:
    # Only needed for serialization typing.
    pass


def default_serialization(val):
    if "numpy" in sys.modules:
        if isinstance(val, np.float64):
            return float(val)

    raise TypeError
