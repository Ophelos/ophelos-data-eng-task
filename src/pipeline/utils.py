"""Utility helpers for the pipeline.

Common functions for file handling, date formatting, and path management.
"""

import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


def get_file_date(filepath: Path) -> str | None:
    """Extract date string from a filename like 'transactions_20240115.csv'.

    Args:
        filepath: Path to the data file.

    Returns:
        Date string (e.g., '20240115') or None if pattern doesn't match.
    """
    stem = filepath.stem  # e.g., 'transactions_20240115'
    parts = stem.rsplit("_", 1)
    if len(parts) == 2 and parts[1].isdigit() and len(parts[1]) == 8:
        return parts[1]
    return None


def format_date(date_str: str, input_format: str = "%Y%m%d") -> str:
    """Convert a date string to ISO format.

    Args:
        date_str: Date string to parse.
        input_format: strftime format of the input.

    Returns:
        ISO-formatted date string (YYYY-MM-DD).
    """
    dt = datetime.strptime(date_str, input_format)
    return dt.strftime("%Y-%m-%d")


def ensure_directory(path: Path) -> Path:
    """Create directory if it doesn't exist.

    Args:
        path: Directory path to ensure exists.

    Returns:
        The path (for chaining).
    """
    path.mkdir(parents=True, exist_ok=True)
    return path


def list_files(directory: Path, pattern: str) -> list[Path]:
    """List files matching a glob pattern, sorted by name.

    Args:
        directory: Directory to search.
        pattern: Glob pattern (e.g., '*.csv').

    Returns:
        Sorted list of matching file paths.
    """
    if not directory.exists():
        logger.warning("Directory does not exist: %s", directory)
        return []
    files = sorted(directory.glob(pattern))
    logger.info("Found %d files matching '%s' in %s", len(files), pattern, directory)
    return files


def sanitize_string(value: str, max_length: int = 200) -> str:
    """Clean and truncate a string value.

    Args:
        value: String to sanitize.
        max_length: Maximum allowed length.

    Returns:
        Cleaned string.
    """
    if not isinstance(value, str):
        return str(value)
    return value.strip()[:max_length]
