import numpy as np


def default_serialization(val):
    if isinstance(val, np.float64):
        return float(val)

    return str(val)
