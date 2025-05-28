"""
Authentication module for the EarthData Download Tool.

Handles authentication with NASA EarthData using earthaccess and .netrc credentials.
"""

import os
from pathlib import Path
from typing import Optional, Union

import earthaccess

from earthdata_download.src.logger import default_logger as logger


class EarthDataAuth:
    """
    Handles authentication with NASA EarthData.

    Uses earthaccess to authenticate with NASA EarthData using .netrc credentials.
    """

    def __init__(self, netrc_file: Optional[Union[str, Path]] = None):
        """
        Initialize the authentication module.

        Args:
            netrc_file: Path to .netrc file (optional, defaults to ~/.netrc)
        """
        self.netrc_file = netrc_file
        self.auth = None

    def authenticate(self) -> bool:
        """
        Authenticate with NASA EarthData using .netrc credentials.

        Returns:
            True if authentication was successful, False otherwise.
        """
        try:
            logger.info("Authenticating with NASA EarthData...")

            # If netrc_file is provided, ensure it exists
            if self.netrc_file:
                netrc_path = Path(self.netrc_file).expanduser().resolve()
                if not netrc_path.exists():
                    logger.error(f"Netrc file not found: {netrc_path}")
                    return False

                # Set the netrc file path in the environment
                os.environ["NETRC"] = str(netrc_path)
                logger.info(f"Using netrc file: {netrc_path}")

            # Authenticate with earthaccess
            self.auth = earthaccess.login()

            if self.auth:
                logger.info("Successfully authenticated with NASA EarthData")
                return True
            else:
                logger.error("Failed to authenticate with NASA EarthData")
                return False

        except Exception as e:
            logger.exception(f"Error authenticating with NASA EarthData: {str(e)}")
            return False

    def get_auth(self):
        """
        Get the earthaccess auth object.

        Returns:
            The earthaccess auth object or None if not authenticated.
        """
        return self.auth

    def is_authenticated(self) -> bool:
        """
        Check if the user is authenticated.

        Returns:
            True if authenticated, False otherwise.
        """
        return self.auth is not None


def get_default_netrc_path() -> Path:
    """
    Get the default path to the .netrc file.

    Returns:
        Path to the .netrc file.
    """
    return Path.home() / ".netrc"


def check_netrc_exists() -> bool:
    """
    Check if the .netrc file exists in the home directory.

    Returns:
        True if the .netrc file exists, False otherwise.
    """
    netrc_path = get_default_netrc_path()
    exists = netrc_path.exists()

    if not exists:
        logger.warning(f"Netrc file not found: {netrc_path}")

    return exists


def check_netrc_permissions() -> bool:
    """
    Check if the .netrc file has the correct permissions.

    Returns:
        True if the .netrc file has the correct permissions, False otherwise.
    """
    netrc_path = get_default_netrc_path()

    if not netrc_path.exists():
        return False

    # Check permissions on Windows vs other platforms
    if os.name == "nt":  # Windows
        return True  # Windows doesn't have the same permission concept
    else:
        # On Unix-like systems, .netrc should have 600 permissions
        permissions = os.stat(netrc_path).st_mode & 0o777
        is_valid = permissions <= 0o600

        if not is_valid:
            logger.warning(
                f"Netrc file has incorrect permissions: {permissions:o}. "
                f"Should be 600 or more restrictive."
            )

        return is_valid
