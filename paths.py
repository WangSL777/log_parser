"""Paths file."""
import os
import shutil

CURRENT_WORKING_PATH = os.getcwd()
GENERATES_PATH = os.path.join(CURRENT_WORKING_PATH, 'generates')
LOG_FILE = os.path.join(GENERATES_PATH, 'log_parser.log')
REPO_PATH = os.path.dirname(os.path.abspath(__file__))


def safely_create_path(path):
    """Safely create path."""
    if not os.path.exists(path):
        print(f'creating {path} ...')
        os.makedirs(path, exist_ok=True)


def safely_create_empty_folder(path):
    """Safely create an empty folder."""
    if os.path.exists(path):
        print(f'deleting {path} ...')
        shutil.rmtree(path)
    safely_create_path(path)


def ensure_paths_exists():
    """Make sure folder exists."""
    safely_create_path(GENERATES_PATH)