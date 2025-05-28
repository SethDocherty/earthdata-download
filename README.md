# EarthData Download Tool

A Python utility for downloading data from NASA's EarthData platform with support for querying collections, granules, and managing large downloads efficiently.

## Overview

The EarthData Download Tool provides a command-line interface for:

- Searching NASA EarthData collections and granules
- Downloading data files with parallel processing
- Resuming interrupted downloads
- Tracking download statistics
- Retrying failed downloads

It's designed to handle large datasets with efficient caching, parallel downloads, and a robust error recovery system.

## Prerequisites

1. Python 3.8 or higher
2. NASA EarthData Login - Create an account at [https://urs.earthdata.nasa.gov/](https://urs.earthdata.nasa.gov/)
3. `.netrc` file with your EarthData credentials (see setup instructions below)

## Installation

### From source

```bash
git clone https://github.com/sethdocherty/earthdata-download.git
cd earthdata-download
pip install -e .
```

Or with Poetry:

```bash
poetry install
```

## Setup

### Create a .netrc file

Create a `.netrc` file in your home directory with your EarthData credentials:

**Linux/macOS:**

```bash
echo "machine urs.earthdata.nasa.gov login YOUR_USERNAME password YOUR_PASSWORD" >> ~/.netrc
chmod 600 ~/.netrc
```

**Windows:**
Create a file named `_netrc` in your user directory (e.g., `C:\Users\YourUsername\_netrc`) with the following content:

```text
machine urs.earthdata.nasa.gov
login YOUR_USERNAME
password YOUR_PASSWORD
```

## Basic Usage

### Download data from a collection

```bash
python -m earthdata_download.src.cli --shortname GEDI02_B --version 002
```

### Check download statistics

```bash
python -m earthdata_download.src.cli --stats --download-dir ./data
```

### Retry failed downloads

```bash
python -m earthdata_download.src.cli --retry --payload-file ./cache/GEDI02_B_payload.pickle
```

## Command-Line Parameters

| Parameter | Description | Default | Example |
|-----------|-------------|---------|---------|
| `--shortname` | Collection shortname to download | - | `--shortname GEDI02_B` |
| `--version` | Collection version | - | `--version 002` |
| `--download-dir` | Directory to save downloaded files | `./data` | `--download-dir /path/to/download` |
| `--cache-dir` | Directory to cache query results | `./cache` | `--cache-dir /path/to/cache` |
| `--payload-file`[^1] | Path to saved collection payload file | - | `--payload-file ./cache/GEDI02_B_payload.pickle` |
| `--log-file` | Path to log file | - | `--log-file ./earthdata.log` |
| `--log-level` | Logging level | `INFO` | `--log-level DEBUG` |
| `--max-workers` | Maximum number of concurrent downloads | `4` | `--max-workers 8` |
| `--netrc-file` | Path to .netrc file | - | `--netrc-file /path/to/.netrc` |
| `--stats` | Show download statistics | `False` | `--stats` |
| `--retry` | Retry failed downloads | `False` | `--retry` |
| `--temporal` | Temporal range in format 'YYYY-MM-DD,YYYY-MM-DD' | - | `--temporal 2021-01-01,2021-12-31` |
| `--limit` | Maximum number of granules to query | `2000` | `--limit 500` |

[^1]: The **payload** pickle file contains the download links for the granules in the collection and is time-stamped to avoid conflicts with future runs. It is created during the initial query of the collection and can be reused for subsequent downloads.

## Example Workflows

### Basic workflow

1. **Search and download data from a collection:**

   ```bash
   python -m earthdata_download.src.cli --shortname GEDI02_B --version 002
   ```

2. **Check download status:**

   ```bash
   python -m earthdata_download.src.cli --stats --download-dir ./data
   ```

3. **Retry any failed downloads:**

   ```bash
   python -m earthdata_download.src.cli --retry --payload-file ./cache/GEDI02_B_payload.pickle
   ```

### Advanced usage

**Download data for a specific time range:**

```bash
python -m earthdata_download.src.cli --shortname GEDI02_B --version 002 --temporal 2021-01-01,2021-06-30
```

**Speed up downloads with more parallel workers:**

```bash
python -m earthdata_download.src.cli --shortname GEDI02_B --version 002 --max-workers 8
```

**Use a custom location for downloads and cache:**

```bash
python -m earthdata_download.src.cli --shortname GEDI02_B --version 002 --download-dir /mnt/data --cache-dir /mnt/cache
```

## Features

- **Parallel Downloads**: Configurable number of simultaneous downloads
- **Resume Capability**: Automatically resumes interrupted downloads
- **Caching**: Caches query results to reduce API calls
- **Error Handling**: Tracks failed downloads for retry
- **OS Agnostic**: Uses pathlib for cross-platform compatibility
- **Progress Tracking**: Shows download progress and statistics

When you first run the tool with a new collection, it will take some time to query the entire collection of granules. Two pickle files are created during this process:

- A **granule search results file** that contains metadata about the granules.
- A **payload file** that contains just the download links for each granule.

This allows the tool to quickly access download links without needing to re-query the entire collection in future runs.

## Troubleshooting

- **Authentication Errors**: Ensure your .netrc file exists and has the correct format
- **Permission Issues**: On Unix-like systems, ensure your .netrc has 600 permissions
- **Download Failures**: Use the --retry flag to attempt to download failed granules
   > Remember to pass in the correct `--payload-file` for retries

## License

MIT

## Acknowledgments

This tool uses NASA's EarthData and earthaccess API to access and download open Earth science data.
