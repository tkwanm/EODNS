import logging
import logging.handlers
import sys
from datetime import datetime
from pathlib import Path

from .config import settings

def setup_logging():
    """
    Configures logging to output to both the console and a date-stamped log file.
    Example log file name: '20250915_eod_monitor.log'
    """
    log_directory = Path(settings.LOG_DIR)
    log_directory.mkdir(parents=True, exist_ok=True)
    
    # --- MODIFICATION: Create date-stamped filename ---
    date_str = datetime.now().strftime("%Y%m%d")
    log_filename = f"{date_str}_eod_monitor.log"
    log_filepath = log_directory / log_filename
    
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - [%(levelname)s] - %(name)s - (%(filename)s).%(funcName)s(%(lineno)d) - %(message)s"
    )

    # 1. Console Handler (prints to terminal)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    # 2. Simple File Handler (writes to the date-stamped file)
    # This is simpler and more direct than a rotating handler for this use case.
    file_handler = logging.FileHandler(filename=log_filepath, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logging.info(f"Logging configured to write to console and file: {log_filepath}")