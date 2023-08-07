"""Path specific utils."""

# Python imports
from pathlib import Path


def is_absolute_file_path(path: Path) -> bool:
    """Check if the file path is absolute and points to a file.

    :param path: path candidate
    """
    return path.is_absolute() and path.is_file()
