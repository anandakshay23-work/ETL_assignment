"""
Structured logging utility for the ETL pipeline.
Provides dual output: console + rotating file handler.
"""

import logging
import os
from datetime import datetime


def setup_logger(name: str = "etl_pipeline", log_dir: str = "/app/logs") -> logging.Logger:
    """
    Configure and return a structured logger with console + file output.

    Args:
        name: Logger name (used in log messages)
        log_dir: Directory for log files

    Returns:
        Configured logging.Logger instance
    """
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # Prevent duplicate handlers on re-initialization
    if logger.handlers:
        return logger

    # Log format: timestamp | level | module | message
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s.%(funcName)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # Console handler (INFO and above)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # File handler (DEBUG and above — full detail)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"etl_pipeline_{timestamp}.log")
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    logger.info(f"Logger initialized. Log file: {log_file}")
    return logger
