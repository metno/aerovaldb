import simplejson  # type: ignore


def json_dumps_wrapper(obj, **kwargs) -> str:
    """
    Wrapper which calls simplejson.dumps with the correct options, known to work for objects
    returned by Pyaerocom.

    This ensures that nan values are serialized as null to be compliant with the json standard.
    """
    return simplejson.dumps(obj, ignore_nan=True, **kwargs)
