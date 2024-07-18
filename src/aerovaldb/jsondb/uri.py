import os


def get_uri(file_path: str) -> str:
    return str(os.path.realpath(file_path))
