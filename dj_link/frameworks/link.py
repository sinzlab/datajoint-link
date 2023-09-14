"""Contains the link decorator that is used by the user to establish a link."""
from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import asdict
from functools import partial
from typing import Any, Mapping, Optional

from dj_link.adapters.controller import DJController
from dj_link.adapters.custom_types import PrimaryKey
from dj_link.adapters.gateway import DJLinkGateway
from dj_link.adapters.identification import IdentificationTranslator
from dj_link.adapters.present import (
    OperationRecord,
    create_idle_entities_updater,
    create_operation_response_presenter,
)
from dj_link.use_cases.use_cases import UseCases, delete, list_idle_entities, process, pull

from . import DJConfiguration, create_tables
from .facade import DJLinkFacade
from .mixin import create_local_endpoint
from .sequence import IterationCallbackList, create_content_replacer


def create_operation_logger() -> Callable[[OperationRecord], None]:
    """Create a function that logs information about finished operations."""
    logger = logging.getLogger("dj_link[operations]")

    def log(record: OperationRecord) -> None:
        for request in record.requests:
            logger.info(f"Operation requested {asdict(request)}")
        for success in record.successes:
            logger.info(f"Operation succeeded {asdict(success)}")
        for failure in record.failures:
            logger.info(f"Operation failed {asdict(failure)}")

    return log


def create_link(  # noqa: PLR0913
    source_host: str,
    source_schema: str,
    outbound_schema: str,
    outbound_table: str,
    local_schema: str,
    *,
    stores: Optional[Mapping[str, str]] = None,
) -> Callable[[type], Any]:
    """Create a link."""
    if stores is None:
        stores = {}

    def inner(obj: type) -> Any:
        translator = IdentificationTranslator()
        tables = create_tables(
            DJConfiguration(
                source_host, source_schema, outbound_schema, outbound_table, local_schema, obj.__name__, stores
            )
        )
        facade = DJLinkFacade(tables.source, tables.outbound, tables.local)
        gateway = DJLinkGateway(facade, translator)
        source_restriction: IterationCallbackList[PrimaryKey] = IterationCallbackList()
        idle_entities_updater = create_idle_entities_updater(translator, create_content_replacer(source_restriction))
        operation_presenter = create_operation_response_presenter(translator, create_operation_logger())
        handlers = {
            UseCases.PULL: partial(pull, link_gateway=gateway, output_port=operation_presenter),
            UseCases.DELETE: partial(delete, link_gateway=gateway, output_port=operation_presenter),
            UseCases.PROCESS: partial(process, link_gateway=gateway, output_port=operation_presenter),
            UseCases.LISTIDLEENTITIES: partial(
                list_idle_entities, link_gateway=gateway, output_port=idle_entities_updater
            ),
        }
        logging.info("Hello")
        controller = DJController(handlers, translator)
        source_restriction.callback = controller.list_idle_entities

        return create_local_endpoint(controller, tables, source_restriction)

    return inner
