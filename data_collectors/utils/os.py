import os
from inspect import getfile, currentframe
from pathlib import Path


def get_root_dir_path() -> str:
    current_path = getfile(currentframe())
    file_path = Path(os.path.abspath(current_path))

    return str(file_path.parent.parent)
