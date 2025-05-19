import logging
import os
from datetime import datetime

def setup_worker_logger(
    log_dir: str,
    provider_id: str,
    rate_sheet_code: str,
    to_file: bool = False
    ) -> logging.Logger:
    """
    Create a dedicated logger for a specific provider and rate sheet.
    If to_file is True, logs go to a file in log_dir.
    Otherwise, logs go to the console.
    """
    logger_name = f"provider_{provider_id}_{rate_sheet_code}"
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)

    ### THIS NEEDS TO BE DELETED TO TURN LOGGING BACK ON

    return logger

    # Avoid adding duplicate handlers
    if not logger.handlers:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

        if to_file:
            os.makedirs(log_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{logger_name}_{timestamp}.log"
            log_path = os.path.join(log_dir, filename)
            handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
        else:
            handler = logging.StreamHandler()

        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger