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
from dj_link.service.use_cases import (
    OperationResponse,
    ProcessToCompletionResponse,
    ResponseRelay,
    UseCases,
    delete,
    list_idle_entities,
    process,
    process_to_completion,
    pull,
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
        process_relay: ResponseRelay[OperationResponse] = ResponseRelay()
        complete_process_relay: ResponseRelay[ProcessToCompletionResponse] = ResponseRelay()
        process_to_completion_service = partial(
            process_to_completion,
            process_service=partial(process, link_gateway=gateway, output_port=process_relay),
            process_service_relay=process_relay,
            output_port=complete_process_relay,
        )
        handlers = {
            UseCases.PULL: partial(
                pull,
                link_gateway=gateway,
                process_to_completion_service=process_to_completion_service,
                process_to_completion_service_relay=complete_process_relay,
                output_port=lambda x: None,
            ),
            UseCases.DELETE: partial(
                delete,
                link_gateway=gateway,
                process_to_completion_service=process_to_completion_service,
                process_to_completion_service_relay=complete_process_relay,
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
