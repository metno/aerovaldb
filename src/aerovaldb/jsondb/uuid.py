import os


def get_uuid(file_path: str) -> str:
    return str(os.path.realpath(file_path))
