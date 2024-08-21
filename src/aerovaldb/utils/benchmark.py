import time
import logging
from functools import wraps


logger = logging.getLogger(__name__)


def benchmark_function(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        time_start = time.perf_counter()
        result = func(*args, **kwargs)
        time_end = time.perf_counter()
        time_duration = time_end - time_start
        logger.info(f"Execution of {func.__name__} took {time_duration:.3f} seconds")
        return result

    return wrapper
