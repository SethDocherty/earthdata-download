"""
Utility functions for the EarthData Download Tool.
"""

import os
import pickle
import json
from pathlib import Path
from typing import Dict, List, Optional, Union

from earthdata_download.src.logger import default_logger as logger


def ensure_dir_exists(path: Union[str, Path]) -> Path:
    """
    Ensure a directory exists, creating it if necessary.

    Args:
        path: Directory path

    Returns:
        Path object for the directory
    """
    path = Path(path)
    path.mkdir(parents=True, exist_ok=True)
    return path


def save_pickle(data: object, filepath: Union[str, Path]) -> bool:
    """
    Save data to a pickle file.

    Args:
        data: Data to save
        filepath: Path to save file

    Returns:
        True if successful, False otherwise
    """
    try:
        filepath = Path(filepath)

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "wb") as f:
            pickle.dump(data, f)

        logger.debug(f"Saved pickle file: {filepath}")
        return True

    except Exception as e:
        logger.error(f"Failed to save pickle file {filepath}: {str(e)}")
        return False


def load_pickle(filepath: Union[str, Path]) -> Optional[object]:
    """
    Load data from a pickle file.

    Args:
        filepath: Path to pickle file

    Returns:
        Loaded data or None if error occurred
    """
    try:
        filepath = Path(filepath)

        if not filepath.exists():
            logger.error(f"Pickle file not found: {filepath}")
            return None

        with open(filepath, "rb") as f:
            data = pickle.load(f)

        logger.debug(f"Loaded pickle file: {filepath}")
        return data

    except Exception as e:
        logger.error(f"Failed to load pickle file {filepath}: {str(e)}")
        return None


def save_json(data: object, filepath: Union[str, Path]) -> bool:
    """
    Save data to a JSON file.

    Args:
        data: Data to save
        filepath: Path to save file

    Returns:
        True if successful, False otherwise
    """
    try:
        filepath = Path(filepath)

        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        with open(filepath, "w") as f:
            json.dump(data, f, indent=2)

        logger.debug(f"Saved JSON file: {filepath}")
        return True

    except Exception as e:
        logger.error(f"Failed to save JSON file {filepath}: {str(e)}")
        return False


def load_json(filepath: Union[str, Path]) -> Optional[object]:
    """
    Load data from a JSON file.

    Args:
        filepath: Path to JSON file

    Returns:
        Loaded data or None if error occurred
    """
    try:
        filepath = Path(filepath)

        if not filepath.exists():
            logger.error(f"JSON file not found: {filepath}")
            return None

        with open(filepath, "r") as f:
            data = json.load(f)

        logger.debug(f"Loaded JSON file: {filepath}")
        return data

    except Exception as e:
        logger.error(f"Failed to load JSON file {filepath}: {str(e)}")
        return None


def format_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format.

    Args:
        size_bytes: Size in bytes

    Returns:
        Formatted size string
    """
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if size_bytes < 1024.0 or unit == "PB":
            break
        size_bytes /= 1024.0

    return f"{size_bytes:.2f} {unit}"


def format_time(seconds: float) -> str:
    """
    Format time in human-readable format.

    Args:
        seconds: Time in seconds

    Returns:
        Formatted time string
    """
    minutes, seconds = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)

    if hours > 0:
        return f"{int(hours)}h {int(minutes)}m {int(seconds)}s"
    elif minutes > 0:
        return f"{int(minutes)}m {int(seconds)}s"
    else:
        return f"{seconds:.2f}s"
