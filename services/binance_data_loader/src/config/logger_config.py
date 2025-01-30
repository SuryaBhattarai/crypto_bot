"""
Module for setting up a JSON-formatted logger with a rotating file handler.

This module contains the `setup_logger` function, which configures a logger for the application.
The logger outputs log messages in JSON format and includes a rotating file handler to manage log
file size. When the file reaches the specified size, it rotates, retaining a set number of backups.

Dependencies:
    - logging
    - logging.handlers
    - python-json-logger
    - pathlib

Example:
    Configure a logger to save log files in a specified directory:

    ```python
    from pathlib import Path
    from src.helper.logger_config import setup_logger

    log_dir = Path("/path/to/logs")
    logger = setup_logger(log_dir)
    logger.info("Logger is configured and operational.")
    ```
"""

import logging
from logging.handlers import RotatingFileHandler
from pythonjsonlogger import jsonlogger
from pathlib import Path

def setup_logger(log_dir: Path) -> logging.Logger:
    """
    Sets up a JSON-formatted logger with a rotating file handler for the application.

    This function initializes a logger named "crypto_bot" that writes log entries to a file
    in JSON format, with automatic log file rotation when a file reaches a specified size.

    Parameters
    ----------
    log_dir : pathlib.Path
        Path to the directory where log files will be saved. If it does not exist, it will be created.

    Returns
    -------
    logging.Logger
        Configured logger instance with a rotating file handler.

    Notes
    -----
    - The logger is configured to log messages at the INFO level and above.
    - A `RotatingFileHandler` is used to manage the log files, with a maximum file size of 1 MB
      and a backup count of 3 files, retaining the 3 most recent logs.
    - Log messages are formatted in JSON, making them easy to parse programmatically or ingest
      into logging aggregation tools.

    Examples
    --------
    Set up and test the logger configuration:

    ```python
    from pathlib import Path
    from src.helper.logger_config import setup_logger

    log_dir = Path("/path/to/logs")
    logger = setup_logger(log_dir)
    logger.info("Logger is configured and operational.")
    ```

    """

    # Ensure that the log directory exists
    if not log_dir.exists():
        log_dir.mkdir(parents=True)

    # Initialize the logger
    logger = logging.getLogger("crypto_bot")
    logger.setLevel(logging.INFO)

    # Set up rotating file handler and JSON formatting
    log_handler = RotatingFileHandler(log_dir / "crypto_bot.log", maxBytes=1_000_000, backupCount=3)
    formatter = jsonlogger.JsonFormatter('%(asctime)s %(name)s %(levelname)s %(message)s')
    log_handler.setFormatter(formatter)

    # Avoid adding duplicate handlers
    if not logger.handlers:
        logger.addHandler(log_handler)

    return logger
