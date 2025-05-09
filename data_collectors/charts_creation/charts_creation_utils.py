import os

from data_collectors.utils.os import get_root_dir_path


def get_resource_path(file_name: str) -> str:
    root_path = get_root_dir_path()
    return os.path.join(root_path, "charts_creation", "resources", file_name)
