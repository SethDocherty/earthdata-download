"""
Logger module for the EarthData Download Tool.
"""

import json
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union


class EarthDataLogger:
    """
    Configurable logger for the EarthData Download Tool.

    Provides structured logging with console and optional file output.
    Supports JSON formatting and log rotation.
    """

    DEFAULT_LOG_FORMAT = "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    DEFAULT_LOG_LEVEL = logging.INFO

    def __init__(
        self,
        name: str = "earthdata_download",
        log_level: Union[int, str] = DEFAULT_LOG_LEVEL,
        log_file: Optional[str] = None,
        log_format: str = DEFAULT_LOG_FORMAT,
        json_logs: bool = True,
        max_bytes: int = 10 * 1024 * 1024,  # 10MB
        backup_count: int = 5,
    ):
        """
        Initialize the logger.

        Args:
            name: Logger name
            log_level: Logging level (INFO, DEBUG, etc.)
            log_file: Path to log file (optional)
            log_format: Format string for log messages
            json_logs: Whether to format logs as JSON
            max_bytes: Max size for log files before rotation
            backup_count: Number of rotated log files to keep
        """
        self.logger = logging.getLogger(name)

        # Convert string log level to numeric if needed
        if isinstance(log_level, str):
            log_level = getattr(logging, log_level.upper(), logging.INFO)

        self.logger.setLevel(log_level)
        self.json_logs = json_logs

        # Create console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)

        # Add formatter to console handler
        formatter = self._get_formatter(log_format)
        console_handler.setFormatter(formatter)

        # Add console handler to logger
        self.logger.addHandler(console_handler)

        # Add file handler if specified
        if log_file:
            log_path = Path(log_file)
            if log_path.parent != Path("."):
                log_path.parent.mkdir(parents=True, exist_ok=True)

            file_handler = RotatingFileHandler(
                log_path, maxBytes=max_bytes, backupCount=backup_count
            )
            file_handler.setLevel(log_level)
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)

    def _get_formatter(self, log_format: str):
        """Get formatter for logs based on configuration."""
        if self.json_logs:
            return JsonFormatter(log_format)
        return logging.Formatter(log_format)

    def debug(self, message: str, **kwargs):
        """Log a debug message."""
        self.logger.debug(message, extra=kwargs)

    def info(self, message: str, **kwargs):
        """Log an info message."""
        self.logger.info(message, extra=kwargs)

    def warning(self, message: str, **kwargs):
        """Log a warning message."""
        self.logger.warning(message, extra=kwargs)

    def error(self, message: str, **kwargs):
        """Log an error message."""
        self.logger.error(message, extra=kwargs)

    def exception(self, message: str, **kwargs):
        """Log an exception message with stack trace."""
        self.logger.exception(message, extra=kwargs)

    def log_download_start(self, granule_name: str, url: str):
        """Log the start of a granule download."""
        self.info(
            f"Starting download for granule: {granule_name}",
            granule_name=granule_name,
            url=url,
            timestamp=datetime.now().isoformat(),
        )

    def log_download_complete(
        self, granule_name: str, elapsed_time: float, file_size: int
    ):
        """Log the completion of a granule download."""
        self.info(
            f"Download complete for granule: {granule_name}",
            granule_name=granule_name,
            elapsed_time=elapsed_time,
            file_size=file_size,
            timestamp=datetime.now().isoformat(),
        )

    def log_download_error(self, granule_name: str, error: str):
        """Log a download error."""
        self.error(
            f"Download error for granule: {granule_name}. Error: {error}",
            granule_name=granule_name,
            error=error,
            timestamp=datetime.now().isoformat(),
        )


class JsonFormatter(logging.Formatter):
    """
    Formatter for JSON structured logs.
    """

    def format(self, record):
        log_data = {
            "timestamp": self.formatTime(record, self.datefmt),
            "name": record.name,
            "level": record.levelname,
            "message": record.getMessage(),
        }

        # Add extra fields from record
        if hasattr(record, "__dict__"):
            for key, value in record.__dict__.items():
                if key not in {
                    "args",
                    "asctime",
                    "created",
                    "exc_info",
                    "exc_text",
                    "filename",
                    "funcName",
                    "id",
                    "levelname",
                    "levelno",
                    "lineno",
                    "module",
                    "msecs",
                    "message",
                    "msg",
                    "name",
                    "pathname",
                    "process",
                    "processName",
                    "relativeCreated",
                    "stack_info",
                    "thread",
                    "threadName",
                }:
                    log_data[key] = value

        # Add exception info if available
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_data)


# Create a default logger instance
default_logger = EarthDataLogger()
