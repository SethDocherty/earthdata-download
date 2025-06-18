"""
Download module for the EarthData Download Tool.

Handles downloading files from NASA EarthData.
"""

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from earthdata_download.src.auth import EarthDataAuth
from earthdata_download.src.logger import get_logger

logger = get_logger()


class EarthDataDownloader:
    """
    Handles downloading files from NASA EarthData.

    Provides methods for downloading individual files and
    processing collection payloads with multiple granules.
    Supports parallel downloads, resume capability, and progress tracking.
    """

    def __init__(
        self,
        auth: EarthDataAuth,
        download_dir: Optional[Union[str, Path]] = None,
        max_workers: int = 4,
        state_file: Optional[Union[str, Path]] = None,
        error_file: Optional[Union[str, Path]] = None,
    ):
        """
        Initialize the downloader.

        Args:
            auth: EarthDataAuth instance
            download_dir: Directory to save downloaded files
            max_workers: Maximum number of concurrent downloads
            state_file: File to save download state
            error_file: File to save download errors
        """
        self.auth = auth
        self.download_dir = Path(download_dir) if download_dir else Path.cwd() / "data"
        self.download_dir.mkdir(parents=True, exist_ok=True)

        self.max_workers = max_workers

        # State files
        self.state_file = (
            Path(state_file)
            if state_file
            else self.download_dir / "download_state.json"
        )
        self.error_file = (
            Path(error_file)
            if error_file
            else self.download_dir / "download_errors.json"
        )

        # Initialize state
        self.completed_granules: Set[str] = set()
        self.errored_granules: Dict[str, str] = {}

        # Load existing state if available
        self._load_state()

        # Configure session with retry
        self.session = self._create_session_with_retry()

        logger.info(
            f"Initialized EarthDataDownloader with download directory: {self.download_dir}, "
            f"max workers: {self.max_workers}"
        )

    def _create_session_with_retry(self):
        """Create a requests session with retry capability."""
        session = requests.Session()
        retry_strategy = Retry(
            total=5,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session

    def _load_state(self):
        """Load download state from state files."""
        # Load completed granules
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    self.completed_granules = set(json.load(f))
                logger.info(
                    f"Loaded {len(self.completed_granules)} completed granules from {self.state_file}"
                )
            except Exception as e:
                logger.warning(f"Failed to load completed granules: {str(e)}")

        # Load errored granules
        if self.error_file.exists():
            try:
                with open(self.error_file, "r") as f:
                    self.errored_granules = json.load(f)
                logger.info(
                    f"Loaded {len(self.errored_granules)} errored granules from {self.error_file}"
                )
            except Exception as e:
                logger.warning(f"Failed to load errored granules: {str(e)}")

    def _save_state(self):
        """Save download state to state files."""
        # Save completed granules
        try:
            with open(self.state_file, "w") as f:
                json.dump(list(self.completed_granules), f)
            logger.debug(
                f"Saved {len(self.completed_granules)} completed granules to {self.state_file}"
            )
        except Exception as e:
            logger.warning(f"Failed to save completed granules: {str(e)}")

        # Save errored granules
        try:
            with open(self.error_file, "w") as f:
                json.dump(self.errored_granules, f)
            logger.debug(
                f"Saved {len(self.errored_granules)} errored granules to {self.error_file}"
            )
        except Exception as e:
            logger.warning(f"Failed to save errored granules: {str(e)}")

    def download_file(self, url: str, target_dir: Union[str, Path]) -> Tuple[bool, str]:
        """
        Download a single file from a URL.

        Args:
            url: URL to download
            target_dir: Directory to save the file

        Returns:
            Tuple of (success, filepath or error message)
        """
        target_dir = Path(target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)

        # Extract filename from URL
        filename = url.split("/")[-1]
        filepath = target_dir / filename

        # Skip if file already exists and has content
        if filepath.exists() and filepath.stat().st_size > 0:
            logger.info(f"File already exists: {filepath}")
            return True, str(filepath)

        # Ensure authentication
        if not self.auth.is_authenticated():
            if not self.auth.authenticate():
                return False, "Authentication failed"

        try:
            logger.info(f"Downloading {url} to {filepath}")
            start_time = time.time()

            # Stream the download to handle large files
            with self.session.get(url, stream=True) as response:
                response.raise_for_status()

                with open(filepath, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

            elapsed_time = time.time() - start_time
            file_size = filepath.stat().st_size

            logger.info(
                f"Download complete: {filepath} ({file_size / 1024 / 1024:.2f} MB in {elapsed_time:.2f} seconds)"
            )

            return True, str(filepath)

        except Exception as e:
            error_msg = f"Failed to download {url}: {str(e)}"
            logger.error(error_msg)

            # Remove partial file if it exists
            if filepath.exists():
                try:
                    filepath.unlink()
                except Exception:
                    pass

            return False, error_msg

    def download_granule(self, granule_name: str, urls: List[str]) -> bool:
        """
        Download all files for a granule.

        Args:
            granule_name: Name of the granule
            urls: List of URLs to download

        Returns:
            True if all downloads were successful, False otherwise
        """
        if not urls:
            logger.warning(f"No URLs provided for granule {granule_name}")
            return False

        # Skip if already completed
        if granule_name in self.completed_granules:
            logger.info(f"Granule {granule_name} already downloaded, skipping")
            return True

        # Create directory for granule using the full granule name
        granule_dir = self.download_dir / granule_name
        granule_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"Downloading granule {granule_name} to {granule_dir}")
        start_time = time.time()

        # Download each URL for the granule
        success = True
        downloaded_files = []

        for url in urls:
            success_url, result = self.download_file(url, granule_dir)
            if success_url:
                downloaded_files.append(result)
            else:
                success = False
                self.errored_granules[granule_name] = result
                self._save_state()
                break

        # If all downloads were successful, mark granule as completed
        if success:
            elapsed_time = time.time() - start_time
            self.completed_granules.add(granule_name)

            # Calculate total file size
            total_size = sum(Path(file).stat().st_size for file in downloaded_files)

            logger.info(
                f"Granule {granule_name} download complete: {len(downloaded_files)} files, "
                f"{total_size / 1024 / 1024:.2f} MB in {elapsed_time:.2f} seconds"
            )

            self._save_state()

        return success

    def download_collection(self, collection_payload: Dict) -> Dict:
        """
        Download all granules in a collection payload.

        Args:
            collection_payload: Collection payload dictionary

        Returns:
            Dictionary with download statistics
        """
        if not collection_payload:
            logger.warning("Empty collection payload, nothing to download")
            return {"total": 0, "completed": 0, "failed": 0}

        # Ensure authentication
        if not self.auth.is_authenticated():
            if not self.auth.authenticate():
                logger.error("Authentication failed, cannot download collection")
                return {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "error": "Authentication failed",
                }

        # Extract collection name and granules
        collection_name = list(collection_payload)[0]
        granules = collection_payload[collection_name]

        total_granules = len(granules)
        logger.info(
            f"Starting download of collection {collection_name} with {total_granules} granules"
        )

        start_time = time.time()
        completed_count = 0
        failed_count = 0

        # Create a list of granules that haven't been completed yet
        remaining_granules = {
            name: urls
            for name, urls in granules.items()
            if name not in self.completed_granules
        }

        if len(remaining_granules) < total_granules:
            logger.info(
                f"Skipping {total_granules - len(remaining_granules)} already downloaded granules"
            )

        if not remaining_granules:
            logger.info("All granules already downloaded")
            return {
                "total": total_granules,
                "completed": total_granules,
                "failed": 0,
                "elapsed_time": 0,
            }

        # Download granules in parallel
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_granule = {
                executor.submit(self.download_granule, name, urls): name
                for name, urls in remaining_granules.items()
            }

            # Process completed tasks
            for future in as_completed(future_to_granule):
                granule_name = future_to_granule[future]
                try:
                    success = future.result()
                    if success:
                        completed_count += 1
                    else:
                        failed_count += 1
                except Exception as e:
                    logger.exception(
                        f"Error downloading granule {granule_name}: {str(e)}"
                    )
                    failed_count += 1
                    self.errored_granules[granule_name] = str(e)

                # Update status
                total_count = completed_count + failed_count
                logger.info(
                    f"Progress: {total_count}/{len(remaining_granules)} granules processed "
                    f"({completed_count} completed, {failed_count} failed)"
                )

        # Calculate statistics
        elapsed_time = time.time() - start_time
        completed_total = len(self.completed_granules)
        failed_total = len(self.errored_granules)

        logger.info(
            f"Download complete for collection {collection_name}: "
            f"{completed_count}/{len(remaining_granules)} granules completed in {elapsed_time:.2f} seconds"
        )

        # Calculate total download size
        total_size = self._calculate_total_download_size()

        # Save final state
        self._save_state()

        # Return download statistics
        return {
            "total": total_granules,
            "completed": completed_total,
            "failed": failed_total,
            "elapsed_time": elapsed_time,
            "total_size_mb": total_size / 1024 / 1024,
            "total_size_gb": total_size / 1024 / 1024 / 1024,
            "total_size_tb": total_size / 1024 / 1024 / 1024 / 1024,
        }

    def _calculate_total_download_size(self) -> float:
        """Calculate the total size of downloaded files in bytes."""
        total_size = 0
        try:
            for path in self.download_dir.rglob("*"):
                if path.is_file() and not path.name.endswith(
                    ".json"
                ):  # Skip state files
                    total_size += path.stat().st_size
        except Exception as e:
            logger.warning(f"Error calculating total download size: {str(e)}")

        return total_size

    def get_download_stats(self) -> Dict:
        """
        Get download statistics.

        Returns:
            Dictionary with download statistics
        """
        total_size = self._calculate_total_download_size()

        return {
            "completed_granules": len(self.completed_granules),
            "failed_granules": len(self.errored_granules),
            "total_download_size_mb": total_size / 1024 / 1024,
            "total_download_size_gb": total_size / 1024 / 1024 / 1024,
            "total_download_size_tb": total_size / 1024 / 1024 / 1024 / 1024,
        }

    def check_missing_granules(
        self, collection_payload: Dict, download_missing: bool = False
    ) -> Dict:
        """
        Check for missing granules in the download directory and optionally download them.

        Args:
            collection_payload: Collection payload dictionary
            download_missing: If True, download the missing granules after identifying them

        Returns:
            Dictionary with missing granule statistics
        """
        if not collection_payload:
            logger.warning("Empty collection payload, nothing to check")
            return {"total": 0, "missing": 0, "downloaded": 0, "failed": 0}

        collection_name = list(collection_payload.keys())[0]
        granules = collection_payload[collection_name]

        logger.info(f"Checking for missing granules in collection {collection_name}")

        missing_granules = []

        for granule_name, urls in granules.items():
            granule_dir = self.download_dir / granule_name

            # Check if granule directory is missing
            if not granule_dir.exists():
                missing_granules.append(granule_name)
                logger.debug(f"Missing granule directory: {granule_name}")
                continue

            # Check if directory is empty
            files_in_dir = list(granule_dir.iterdir())
            if not files_in_dir:
                missing_granules.append(granule_name)
                logger.debug(f"Empty granule directory: {granule_name}")
                continue

            # Check if all expected files have been downloaded
            expected_files = set()
            for url in urls:
                filename = url.split("/")[-1]
                expected_files.add(filename)

            actual_files = set()
            for file_path in files_in_dir:
                if file_path.is_file() and file_path.stat().st_size > 0:
                    actual_files.add(file_path.name)

            # If not all expected files are present, consider it missing
            if not expected_files.issubset(actual_files):
                missing_granules.append(granule_name)
                missing_files = expected_files - actual_files
                logger.debug(
                    f"Incomplete granule {granule_name}, missing files: {missing_files}"
                )

        # Save missing granules to JSON file
        missing_granules_file = self.download_dir / "missing_granules.json"
        try:
            with open(missing_granules_file, "w") as f:
                json.dump(missing_granules, f, indent=2)
            logger.info(
                f"Saved {len(missing_granules)} missing granules to {missing_granules_file}"
            )
        except Exception as e:
            logger.warning(f"Failed to save missing granules file: {str(e)}")

        # Remove missing granules from completed state so they can be re-downloaded
        if missing_granules:
            removed_count = 0
            for granule_name in missing_granules:
                if granule_name in self.completed_granules:
                    self.completed_granules.remove(granule_name)
                    removed_count += 1

            if removed_count > 0:
                logger.info(f"Removed {removed_count} granules from completed state")
                self._save_state()

        # Download missing granules if requested
        downloaded_count = 0
        failed_count = 0

        if download_missing and missing_granules:
            logger.info(
                f"Starting download of {len(missing_granules)} missing granules"
            )
            start_time = time.time()

            for granule_name in missing_granules:
                if granule_name in granules:
                    urls = granules[granule_name]
                    success = self.download_granule(granule_name, urls)
                    if success:
                        downloaded_count += 1
                    else:
                        failed_count += 1

            elapsed_time = time.time() - start_time
            logger.info(
                f"Missing granules download complete: {downloaded_count}/{len(missing_granules)} "
                f"succeeded in {elapsed_time:.2f} seconds"
            )

        return {
            "total": len(granules),
            "missing": len(missing_granules),
            "downloaded": downloaded_count,
            "failed": failed_count,
        }

    def retry_failed_granules(self, collection_payload: Dict) -> Dict:
        """
        Retry downloading failed granules.

        Args:
            collection_payload: Collection payload dictionary

        Returns:
            Dictionary with retry statistics
        """
        if not self.errored_granules:
            logger.info("No failed granules to retry")
            return {"retried": 0, "succeeded": 0, "failed": 0}

        collection_name = list(collection_payload.keys())[0]
        granules = collection_payload[collection_name]

        # Filter granules to only include failed ones
        retry_granules = {
            name: urls
            for name, urls in granules.items()
            if name in self.errored_granules
        }

        if not retry_granules:
            logger.warning("Failed granules not found in collection payload")
            return {"retried": 0, "succeeded": 0, "failed": 0}

        logger.info(f"Retrying {len(retry_granules)} failed granules")

        # Clear error status for retried granules
        for name in retry_granules:
            if name in self.errored_granules:
                del self.errored_granules[name]

        self._save_state()

        # Download retried granules
        start_time = time.time()
        succeeded = 0
        failed = 0

        for name, urls in retry_granules.items():
            success = self.download_granule(name, urls)
            if success:
                succeeded += 1
            else:
                failed += 1

        elapsed_time = time.time() - start_time

        logger.info(
            f"Retry complete: {succeeded}/{len(retry_granules)} granules succeeded "
            f"in {elapsed_time:.2f} seconds"
        )

        return {
            "retried": len(retry_granules),
            "succeeded": succeeded,
            "failed": failed,
            "elapsed_time": elapsed_time,
        }
