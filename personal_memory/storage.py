"""Fresh-install SQLite naming compatible with Hermes native backup detection."""
from pathlib import Path


def database_path(directory,stem):
    return Path(directory)/(stem+'.db')
