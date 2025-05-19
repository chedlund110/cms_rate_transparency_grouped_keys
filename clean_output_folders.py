import os
import shutil
from shared_config import SharedConfig

def clear_output_folders(shared_config: SharedConfig, preserve_extensions: set[str] = {'.py'}) -> None:
    temp_output_path = shared_config.directory_structure["temp_output_dir"]

    if os.path.exists(temp_output_path):
        for root, dirs, files in os.walk(temp_output_path, topdown=False):
            for name in files:
                file_path = os.path.join(root, name)
                if not any(name.endswith(ext) for ext in preserve_extensions):
                    os.remove(file_path)
            for name in dirs:
                dir_path = os.path.join(root, name)
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)

    # Optionally clear top-level merged TXT/MMS files (outside temp_output)
    """
    merged_extensions = {".TXT", ".MMS"}
    for filename in os.listdir(shared_config.mrf_target_directory):
        if any(filename.endswith(ext) for ext in merged_extensions):
            full_path = os.path.join(shared_config.mrf_target_directory, filename)
            os.remove(full_path)
"""