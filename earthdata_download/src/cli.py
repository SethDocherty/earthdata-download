"""
Command-line interface module for the EarthData Download Tool.

Provides a CLI for downloading data from NASA EarthData.
"""

import argparse
import sys
import textwrap
from pathlib import Path
from typing import Dict

from earthdata_download.src.auth import (
    EarthDataAuth,
    check_netrc_exists,
    check_netrc_permissions,
)
from earthdata_download.src.download import EarthDataDownloader
from earthdata_download.src.logger import get_logger
from earthdata_download.src.query import EarthDataQuery

# Global logger variable - will be initialized in main()
logger = None


def create_parser() -> argparse.ArgumentParser:
    """Create command-line argument parser."""
    parser = argparse.ArgumentParser(
        description="EarthData Download Tool - Download data from NASA EarthData",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=textwrap.dedent(
            """
            Example usage:
              # Download data from a collection
              python -m earthdata_download.src.cli --shortname GEDI02_B --version 002

              # Specify a custom download directory
              python -m earthdata_download.src.cli --shortname GEDI02_B --download-dir /path/to/download

              # Load an existing payload file
              python -m earthdata_download.src.cli --payload-file ./cache/GEDI02_B_payload.pickle

              # Set number of concurrent downloads
              python -m earthdata_download.src.cli --shortname GEDI02_B --max-workers 8

              # View download statistics
              python -m earthdata_download.src.cli --stats --download-dir /path/to/download

              # Retry failed downloads
              python -m earthdata_download.src.cli --retry --payload-file ./cache/GEDI02_B_payload.pickle
        """
        ),
    )

    parser.add_argument(
        "--shortname",
        help="Collection shortname to download (e.g., GEDI02_B)",
    )

    parser.add_argument(
        "--version",
        help="Collection version (e.g., 002)",
    )

    parser.add_argument(
        "--download-dir",
        help="Directory to save downloaded files",
        default="./data",
    )

    parser.add_argument(
        "--cache-dir",
        help="Directory to cache query results",
        default="./cache",
    )

    parser.add_argument(
        "--payload-file",
        help="Path to saved collection payload file",
    )

    parser.add_argument(
        "--granule-payload-file",
        help="Path to saved granules payload file (for building collection payload)",
    )

    parser.add_argument(
        "--log-file",
        help="Path to log file",
    )

    parser.add_argument(
        "--log-level",
        help="Logging level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )

    parser.add_argument(
        "--max-workers",
        help="Maximum number of concurrent downloads",
        type=int,
        default=4,
    )

    parser.add_argument(
        "--netrc-file",
        help="Path to .netrc file",
    )

    parser.add_argument(
        "--stats",
        help="Show download statistics",
        action="store_true",
    )

    parser.add_argument(
        "--retry",
        help="Retry failed downloads",
        action="store_true",
    )

    parser.add_argument(
        "--temporal",
        help="Temporal range in format 'YYYY-MM-DD,YYYY-MM-DD'",
    )

    parser.add_argument(
        "--limit",
        help="Maximum number of granules to query",
        type=int,
        default=2000,
    )

    return parser


def show_download_stats(download_dir: str):
    """Show download statistics."""
    download_dir = Path(download_dir)

    if not download_dir.exists():
        default_logger.error(f"Download directory not found: {download_dir}")
        return

    # Create auth and downloader objects
    auth = EarthDataAuth()
    downloader = EarthDataDownloader(auth, download_dir=download_dir)

    # Get stats
    stats = downloader.get_download_stats()

    # Print stats
    print("\nEarthData Download Statistics")
    print("============================")
    print(f"Completed granules: {stats['completed_granules']}")
    print(f"Failed granules: {stats['failed_granules']}")
    print(
        f"Total download size: {stats['total_download_size_mb']:.2f} MB "
        f"({stats['total_download_size_gb']:.2f} GB, "
        f"{stats['total_download_size_tb']:.6f} TB)"
    )
    print("============================\n")


def parse_temporal(temporal_str: str) -> Dict:
    """Parse temporal range string into start and end dates."""
    if not temporal_str:
        return None

    try:
        start_date, end_date = temporal_str.split(",")
        start_date = start_date.strip()
        end_date = end_date.strip()

        return {
            "start_date": start_date,
            "end_date": end_date,
        }
    except ValueError:
        default_logger.error(
            "Invalid temporal range format. Expected 'YYYY-MM-DD,YYYY-MM-DD'"
        )
        return None


def retry_failed_downloads(payload_file: str, download_dir: str, max_workers: int):
    """Retry failed downloads."""
    if not payload_file:
        default_logger.error("Payload file is required for retry operation")
        return

    payload_path = Path(payload_file)
    if not payload_path.exists():
        default_logger.error(f"Payload file not found: {payload_path}")
        return

    # Create objects
    auth = EarthDataAuth()
    if not auth.authenticate():
        default_logger.error("Authentication failed")
        return

    # Load payload
    query = EarthDataQuery(auth)
    payload = query.load_collection_payload(payload_path)

    if not payload:
        default_logger.error("Failed to load payload or payload is empty")
        return

    # Create downloader and retry failed granules
    downloader = EarthDataDownloader(
        auth, download_dir=download_dir, max_workers=max_workers
    )

    retry_stats = downloader.retry_failed_granules(payload)

    # Print retry stats
    print("\nRetry Statistics")
    print("================")
    print(f"Retried granules: {retry_stats['retried']}")
    print(f"Succeeded: {retry_stats['succeeded']}")
    print(f"Failed: {retry_stats['failed']}")
    print(f"Elapsed time: {retry_stats['elapsed_time']:.2f} seconds")
    print("================\n")


def main():
    """Main entry point for the CLI."""
    global logger  # Declare logger as global

    parser = create_parser()
    args = parser.parse_args()

    # At the start of your application
    logger = get_logger(
        reconfigure=True, log_level=args.log_level, log_file=args.log_file
    )
    logger.info("Application starting...")

    # Show stats if requested
    if args.stats:
        show_download_stats(args.download_dir)
        return

    # Retry failed downloads if requested
    if args.retry:
        retry_failed_downloads(args.payload_file, args.download_dir, args.max_workers)
        return

    # Check for required arguments
    if not args.payload_file and not args.shortname:
        logger.error("Either --shortname or --payload-file is required")
        parser.print_help()
        sys.exit(1)

    # Check for .netrc file
    if not check_netrc_exists():
        logger.error(
            "No .netrc file found in home directory. Please create one with your "
            "EarthData credentials."
        )
        sys.exit(1)

    # Check .netrc permissions
    if not check_netrc_permissions():
        logger.warning(
            "Your .netrc file may have incorrect permissions. "
            "On Unix-like systems, it should be 600 (read/write for owner only)."
        )

    # Create authentication object
    auth = EarthDataAuth(netrc_file=args.netrc_file)
    if not auth.authenticate():
        logger.error("Authentication failed")
        sys.exit(1)

    # Create cache directory
    cache_dir = Path(args.cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)

    # Create download directory
    download_dir = Path(args.download_dir)
    download_dir.mkdir(parents=True, exist_ok=True)

    # Create EarthDataQuery object
    query = EarthDataQuery(auth, cache_dir=cache_dir)

    # Get collection payload
    collection_payload = {}

    if args.granule_payload_file:
        logger.info(f"Loading granules from {args.payload_file}")
        granules_payload = query.load_granules_payload(args.granule_payload_file)

        if not granules_payload:
            logger.error("Failed to load granules payload")
            sys.exit(1)

        # rebuild collection payload from granules
        collection_payload = query.build_collection_payload(granules_payload)
        if not collection_payload:
            logger.error("Failed to build collection payload from granules")
            sys.exit(1)

        # Save collection payload for future use
        payload_file = query.save_collection_payload(collection_payload)

    elif args.payload_file:
        # Load existing payload file
        logger.info(f"Loading collection payload from {args.payload_file}")
        collection_payload = query.load_collection_payload(args.payload_file)
        if not collection_payload:
            logger.error("Failed to load collection payload")
            sys.exit(1)
    else:
        # Query granules directly (no need for separate collection query first)
        logger.info(f"Searching for granules in collection: {args.shortname}")

        # Parse temporal range
        temporal = parse_temporal(args.temporal)

        # Search for granules using search_data
        granules_dict = query.search_granules(
            args.shortname,
            version=args.version,
            temporal=temporal,
        )

        if not granules_dict:
            logger.error(f"No granules found for collection: {args.shortname}")
            sys.exit(1)

        collection_shortname = list(granules_dict.keys())[0]
        granules_count = len(granules_dict[collection_shortname])
        logger.info(
            f"Found {granules_count} granules for collection: {collection_shortname}"
        )

        # Build collection payload with URLs for each granule
        logger.info("Building collection payload with download URLs")
        collection_payload = query.build_collection_payload(granules_dict)

        if not collection_payload:
            logger.error("Failed to build collection payload")
            sys.exit(1)

        granule_count = len(collection_payload[collection_shortname])
        logger.info(
            f"Created payload with {granule_count} granules containing download URLs"
        )

        # Save collection payload for future use or resume
        payload_file = query.save_collection_payload(collection_payload)
        logger.info(f"Saved collection payload to {payload_file}")

    # Create downloader
    downloader = EarthDataDownloader(
        auth,
        download_dir=download_dir,
        max_workers=args.max_workers,
    )

    # Download collection
    collection_name = list(collection_payload.keys())[0]
    granule_count = len(collection_payload[collection_name])
    logger.info(
        f"Starting download of {granule_count} granules from collection {collection_name}"
    )

    stats = downloader.download_collection(collection_payload)

    # Print download statistics
    print("\nDownload Statistics")
    print("==================")
    print(f"Total granules: {stats['total']}")
    print(f"Completed: {stats['completed']}")
    print(f"Failed: {stats['failed']}")
    print(f"Elapsed time: {stats['elapsed_time']:.2f} seconds")
    print(
        f"Total download size: {stats['total_size_mb']:.2f} MB "
        f"({stats['total_size_gb']:.2f} GB, "
        f"{stats['total_size_tb']:.6f} TB)"
    )
    print("==================\n")

    # Suggest retry if there are failed granules
    if stats["failed"] > 0:
        print(f"There were {stats['failed']} failed granules.")
        print("You can retry them using the --retry flag with the payload file:")
        payload_files = list(cache_dir.glob("*_payload.pickle"))
        if payload_files:
            latest_payload = max(payload_files, key=lambda p: p.stat().st_ctime)
            print(
                f"python -m earthdata_download.src.cli --retry --payload-file {latest_payload}"
            )

    return 0


if __name__ == "__main__":
    sys.exit(main())
