import os
from shared_config import SharedConfig

def ensure_directories_exist(shared_config: SharedConfig) -> None:
    """
    Creates all necessary directories for the app to run.
    Ensures logs, temp output, test output, and MRF folders are in place.
    """
    for path in shared_config.directory_structure.values():
        os.makedirs(path, exist_ok=True)
    
    print("✅ All required directories have been created or already exist.")
