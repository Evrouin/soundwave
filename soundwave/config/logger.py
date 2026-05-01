"""Standardized logging for the Soundwave platform."""

import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger with consistent formatting.

    Usage:
        from soundwave.config.logger import get_logger
        logger = get_logger(__name__)
        logger.info("Processing %d tracks", count)
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(
            logging.Formatter("[%(asctime)s] %(levelname)s %(name)s — %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
        )
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger
