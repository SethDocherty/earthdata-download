"""
Query module for the EarthData Download Tool.

Handles querying NASA EarthData Search for collections and granules.
"""

import pickle
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Union

import earthaccess

from earthdata_download.src.auth import EarthDataAuth
from earthdata_download.src.logger import default_logger as logger


class EarthDataQuery:
    """
    Handles querying NASA EarthData Search.

    Provides methods for searching collections and granules.
    Includes functionality to cache results as pickle files.
    """

    def __init__(
        self, auth: EarthDataAuth, cache_dir: Optional[Union[str, Path]] = None
    ):
        """
        Initialize the query module.

        Args:
            auth: EarthDataAuth instance
            cache_dir: Directory to cache query results (optional)
        """
        self.auth = auth
        self.cache_dir = Path(cache_dir) if cache_dir else Path.cwd() / "cache"
        self.cache_dir.mkdir(parents=True, exist_ok=True)

        logger.info(
            f"Initializing EarthDataQuery with cache directory: {self.cache_dir}"
        )

    def search_collection(
        self,
        shortname: str,
        version: Optional[str] = None,
        force_refresh: bool = False,
    ):
        """
        Search for a collection by shortname and version using earthaccess.search_datasets().

        Args:
            shortname: Collection shortname
            version: Collection version (optional)
            force_refresh: Whether to force a fresh query instead of using cache

        Returns:
            List of collection results or None if error occurred
        """
        # Generate cache filename
        cache_key = f"{shortname}"
        if version:
            cache_key = f"{cache_key}_v{version}"

        cache_file = self.cache_dir / f"{cache_key}_collection.pickle"

        # Check cache if not forcing refresh
        if not force_refresh and cache_file.exists():
            try:
                logger.info(
                    f"Loading cached collection data for {shortname} from {cache_file}"
                )
                with open(cache_file, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cached collection data: {str(e)}")

        # If cache doesn't exist or force_refresh, perform query
        try:
            logger.info(f"Searching for collection: {shortname} version: {version}")

            # Ensure authentication
            if not self.auth.is_authenticated():
                if not self.auth.authenticate():
                    logger.error("Authentication required for collection search")
                    return None

            # Build query parameters
            query_params = {"short_name": shortname}
            if version:
                query_params["version"] = version

            # Perform query using earthaccess.search_datasets()
            results = earthaccess.search_datasets(**query_params)

            if results:
                logger.info(f"Found {len(results)} collection results for {shortname}")

                # Cache results
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_file, "wb") as f:
                    pickle.dump(results, f)
                logger.info(f"Cached collection results to {cache_file}")

                return results
            else:
                logger.warning(f"No collections found for {shortname}")
                return []

        except Exception as e:
            logger.exception(f"Error searching for collection {shortname}: {str(e)}")
            return None

    def search_granules(
        self,
        short_name: str,
        version: str,
        temporal: Optional[Dict] = None,
        bounding_box: Optional[List[float]] = None,
        polygon: Optional[List[float]] = None,
        point: Optional[List[float]] = None,
        force_refresh: bool = False,
    ):
        """
        Search for granules in a collection using earthaccess.search_data().

        This method returns granules for the specified collection with optional
        spatial and temporal filtering.

        Args:
            short_name: Collection ID or shortname
            version: dataset version
            temporal: Temporal range as dict with 'start_date' and 'end_date' keys
            bounding_box: Bounding box coordinates [west, south, east, north]
            polygon: Polygon coordinates
            point: Point coordinates [lon, lat]
            force_refresh: Whether to force a fresh query instead of using cache

        Returns:
            Dictionary with collection shortname as key and list of granule results as value,
            or None if error occurred
        """
        # Generate cache key and filename based on query parameters
        cache_key = f"{short_name}_granules"
        if temporal:
            cache_key += f"_{temporal['start_date'].replace('-', '')}_{temporal['end_date'].replace('-', '')}"
        if bounding_box:
            cache_key += f"_bbox"
        if polygon:
            cache_key += f"_polygon"
        if point:
            cache_key += f"_point_{point[0]}_{point[1]}"

        cache_file = self.cache_dir / f"{cache_key}.pickle"

        # Check cache if not forcing refresh
        if not force_refresh and cache_file.exists():
            try:
                logger.info(f"Loading cached granule data from {cache_file}")
                with open(cache_file, "rb") as f:
                    return pickle.load(f)
            except Exception as e:
                logger.warning(f"Failed to load cached granule data: {str(e)}")

        # If cache doesn't exist or force_refresh, perform query
        try:
            logger.info(f"Searching for granules in collection: {short_name}")

            # Ensure authentication
            if not self.auth.is_authenticated():
                if not self.auth.authenticate():
                    logger.error("Authentication required for granule search")
                    return None

            # Build query parameters
            query_params = {"short_name": short_name, "version": version}
            if temporal:
                query_params["temporal"] = temporal
            if bounding_box:
                query_params["bounding_box"] = bounding_box
            if polygon:
                query_params["polygon"] = polygon
            if point:
                query_params["point"] = point

            # Perform query using earthaccess.search_data()
            results = earthaccess.search_data(**query_params)

            if results:
                logger.info(f"Found {len(results)} granule results for {short_name}")

                # Structure the results as a dictionary with collection shortname as key
                # Extract collection shortname (use the provided short_name if can't extract from results)
                if results and hasattr(results[0], "short_name"):
                    collection_shortname = results[0].short_name.split("-")[0]
                else:
                    collection_shortname = short_name.split("-")[0]

                # Create a dictionary with collection shortname as key and results as value
                structured_results = {collection_shortname: results}

                # Cache structured results
                cache_file.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_file, "wb") as f:
                    pickle.dump(structured_results, f)
                logger.info(f"Cached granule results to {cache_file}")

                return structured_results
            else:
                logger.warning(f"No granules found for {short_name}")
                # Return empty dictionary with collection shortname
                return {short_name.split("-")[0]: []}

        except Exception as e:
            logger.exception(f"Error searching for granules in {short_name}: {str(e)}")
            return None

    def build_collection_payload(self, granules_dict) -> Dict:
        """
        Build a collection payload dictionary from granule results.

        The payload has the structure:
        {
            "<collection shortname>": {
                "<granule name>": [
                    "<URL 1>",
                    "<URL 2>",
                    ...
                ]
            }
        }

        Args:
            granules_dict: Dictionary with collection shortname as key and list of granule results as value
                          or a list of granule results (for backward compatibility)

        Returns:
            Dictionary containing granule names and download URLs
        """
        if not granules_dict:
            logger.warning("No granules provided for building collection payload")
            return {}

        try:
            # Check if input is already in the correct dictionary format
            if isinstance(granules_dict, dict):
                # Extract collection shortname and granules
                collection_shortname = list(granules_dict.keys())[0]
                granules = granules_dict[collection_shortname]
                logger.info(
                    f"Building collection payload for {len(granules)} granules from {collection_shortname}"
                )
            else:
                # Backward compatibility: Handle a direct list of granules
                granules = granules_dict
                # Extract collection shortname from the first granule
                if granules and hasattr(granules[0], "short_name"):
                    collection_shortname = granules[0].short_name.split("-")[0]
                    logger.info(
                        f"Building collection payload for {len(granules)} granules"
                    )
                else:
                    logger.error("Cannot extract collection shortname from granules")
                    return {}

            # Initialize payload dictionary
            payload = {collection_shortname: {}}

            # Process each granule
            for granule in granules:
                granule_name = granule["umm"]["GranuleUR"]
                urls = []

                # Extract URLs from related URLs list
                related_urls = granule["umm"]["RelatedUrls"]

                for url_info in related_urls:
                    url_type = url_info.get("Type", "")
                    url = url_info.get("URL", "")

                    # Filter specific patterns
                    if "doi.org" in url:
                        continue
                    if "s3credentials" in url:
                        continue

                    # Only include HTTPS URLs (exclude S3 URLs)
                    if url_type in [
                        "GET DATA",
                        "VIEW RELATED INFORMATION",
                        "GET RELATED VISUALIZATION",
                    ] and url.startswith("https://"):
                        urls.append(url)

                # Add granule to payload if it has URLs
                if urls:
                    payload[collection_shortname][granule_name] = urls
                elif len(urls) < 3:
                    logger.warning(f"Granule {granule_name} has less than 3 URLs")
                else:
                    logger.warning(f"No URLs found for granule {granule_name}")

            logger.info(
                f"Built collection payload with {len(payload[collection_shortname])} granules"
            )

            return payload

        except Exception as e:
            logger.exception(f"Error building collection payload: {str(e)}")
            return {}

    def save_collection_payload(
        self, payload: Dict, filename: Optional[str] = None
    ) -> str:
        """
        Save a collection payload to disk as a pickle file.

        Args:
            payload: Collection payload dictionary
            filename: Filename to save payload (optional)

        Returns:
            Path to the saved file
        """
        if not payload:
            logger.warning("Empty payload, not saving to disk")
            return ""

        try:
            # Generate filename if not provided
            if not filename:
                # Get collection shortname from the first key in the payload
                collection_shortname = list(payload.keys())[0]
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{collection_shortname}_{timestamp}_payload.pickle"

            # Ensure the filename has a .pickle extension
            if not filename.endswith(".pickle"):
                filename += ".pickle"

            # Create full file path
            file_path = self.cache_dir / filename

            # Save payload to disk
            with open(file_path, "wb") as f:
                pickle.dump(payload, f)

            logger.info(f"Saved collection payload to {file_path}")

            return str(file_path)

        except Exception as e:
            logger.exception(f"Error saving collection payload: {str(e)}")
            return ""

    def load_collection_payload(self, file_path: Union[str, Path]) -> Dict:
        """
        Load a collection payload from disk.

        Args:
            file_path: Path to the pickle file

        Returns:
            Collection payload dictionary
        """
        file_path = Path(file_path)

        if not file_path.exists():
            logger.error(f"Payload file not found: {file_path}")
            return {}

        try:
            logger.info(f"Loading collection payload from {file_path}")

            with open(file_path, "rb") as f:
                payload = pickle.load(f)

            return payload

        except Exception as e:
            logger.exception(f"Error loading collection payload: {str(e)}")
            return {}
