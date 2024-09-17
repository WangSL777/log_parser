"""Paths file."""
import os
import shutil

CURRENT_WORKING_PATH = os.getcwd()
GENERATES_PATH = os.path.join(CURRENT_WORKING_PATH, 'generates')
LOG_FILE = os.path.join(GENERATES_PATH, 'log_parser.log')
FIGURE_HTML = os.path.join(GENERATES_PATH, 'figure.html')
FIGURE_PNG = os.path.join(GENERATES_PATH, 'figure.png')
PASER_RESULT_FILE = os.path.join(GENERATES_PATH, 'result.json')

REPO_PATH = os.path.dirname(os.path.abspath(__file__))


def safely_create_path(path):
    """Safely create path."""
    if not os.path.exists(path):
        print(f'creating {path} ...')
        os.makedirs(path, exist_ok=True)


def ensure_paths_exists():
    """Make sure folder exists."""
    safely_create_path(GENERATES_PATH)