"""Contains the link decorator that is used by the user to establish a link."""
from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import Any, Mapping, Optional

from dj_link.adapters.controller import DJController
from dj_link.adapters.custom_types import PrimaryKey
from dj_link.adapters.gateway import DJLinkGateway
from dj_link.adapters.identification import IdentificationTranslator
from dj_link.adapters.present import (
    create_idle_entities_updater,
    create_operation_response_presenter,
)
from dj_link.service.io import make_responsive
from dj_link.service.services import (
    UseCases,
    delete,
    list_idle_entities,
    process,
    process_to_completion,
    pull,
    start_delete_process,
    start_pull_process,
)

from . import DJConfiguration, create_tables
from .facade import DJLinkFacade
from .log import create_operation_logger
from .mixin import create_local_endpoint
from .sequence import IterationCallbackList, create_content_replacer


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
        process_service = partial(
            make_responsive(partial(process, link_gateway=gateway)), output_port=operation_presenter
        )
        start_pull_process_service = partial(
            make_responsive(partial(start_pull_process, link_gateway=gateway)), output_port=operation_presenter
        )
        start_delete_process_service = partial(
            make_responsive(partial(start_delete_process, link_gateway=gateway)), output_port=operation_presenter
        )
        process_to_completion_service = partial(
            make_responsive(partial(process_to_completion, process_service=process_service)), output_port=lambda x: None
        )
        handlers = {
            UseCases.PULL: partial(
                pull,
                process_to_completion_service=process_to_completion_service,
                start_pull_process_service=start_pull_process_service,
                output_port=lambda x: None,
            ),
            UseCases.DELETE: partial(
                delete,
                process_to_completion_service=process_to_completion_service,
                start_delete_process_service=start_delete_process_service,
                output_port=lambda x: None,
            ),
            UseCases.PROCESS: partial(process, link_gateway=gateway, output_port=operation_presenter),
            UseCases.LISTIDLEENTITIES: partial(
                list_idle_entities, link_gateway=gateway, output_port=idle_entities_updater
            ),
        }
        controller = DJController(handlers, translator)
        source_restriction.callback = controller.list_idle_entities

        return create_local_endpoint(controller, tables, source_restriction)

    return inner
