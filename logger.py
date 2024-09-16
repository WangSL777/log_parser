"""Logger module."""
import logging
import os

import paths

_formatter = logging.Formatter(
    '%(asctime)s -  %(levelname)5s - %(name)s:%(lineno)3d - %(message)s')

# console output
_console_logger_handler = logging.StreamHandler()
_console_logger_handler.setLevel(logging.ERROR)
_console_logger_handler.setFormatter(_formatter)

# file output.
paths.safely_create_path(paths.GENERATES_PATH)  # this is needed as in main.py logger is imported at the very beginning
_logging_file = paths.LOG_FILE
_file_logger_handler = logging.FileHandler(_logging_file)
_file_logger_handler.setLevel(logging.DEBUG)
_file_logger_handler.setFormatter(_formatter)


def update_console_level(console_level):
    """Update console log level for console output."""
    _console_logger_handler.setLevel(console_level)


def get_logger(name):
    """Return (create) a logger with the specified name.

    Args:
        - name: logger name.
    """
    # show file path relatively to project root folder
    relative_path = os.path.relpath(name, paths.REPO_PATH)

    logger = logging.getLogger(relative_path)
    logger.setLevel(logging.DEBUG)

    logger.addHandler(_console_logger_handler)
    logger.addHandler(_file_logger_handler)
    return logger


def clear_logs():
    """Clean up logs."""
    # clear all the log file content.
    with open(_logging_file, 'w'):
        pass


def init_logger(log_level, clean=False):
    """Update global logging verbosity.

    Args:
        verbosity: integer, console log level, 0 for error only, 3 for debug level
        clean: clear existing logs if True
    """
    print(f'Logs are stored at {_logging_file}')
    if clean:
        clear_logs()

    console_level = getattr(logging, log_level, logging.INFO)

    print(f'Logging verbosity is set to be {logging.getLevelName(console_level)}.', flush=True)
    update_console_level(console_level)