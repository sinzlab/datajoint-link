"""Logging functionality."""
import logging
from dataclasses import asdict
from typing import Callable

from link.adapters.present import OperationRecord


def create_operation_logger() -> Callable[[OperationRecord], None]:
    """Create a function that logs information about finished operations."""
    logger = logging.getLogger("link[operations]")

    def log(record: OperationRecord) -> None:
        for request in record.requests:
            logger.info(f"Operation requested {asdict(request)}")
        for success in record.successes:
            logger.info(f"Operation succeeded {asdict(success)}")
        for failure in record.failures:
            logger.info(f"Operation failed {asdict(failure)}")

    return log
