"""Contains the link decorator that is used by the user to establish a link."""
from __future__ import annotations

from collections.abc import Callable
from functools import partial
from typing import Any, Mapping, Optional

from link.adapters.controller import DJController
from link.adapters.custom_types import PrimaryKey
from link.adapters.gateway import DJLinkGateway
from link.adapters.identification import IdentificationTranslator
from link.adapters.present import (
    create_idle_entities_updater,
    create_operation_response_presenter,
)
from link.service.io import make_responsive
from link.service.services import (
    Services,
    delete,
    list_idle_entities,
    process,
    process_to_completion,
    pull,
    start_delete_process,
    start_pull_process,
)
from link.service.uow import UnitOfWork

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
        uow = UnitOfWork(gateway)
        source_restriction: IterationCallbackList[PrimaryKey] = IterationCallbackList()
        idle_entities_updater = create_idle_entities_updater(translator, create_content_replacer(source_restriction))
        operation_presenter = create_operation_response_presenter(translator, create_operation_logger())
        process_service = partial(make_responsive(partial(process, uow=uow)), output_port=operation_presenter)
        start_pull_process_service = partial(
            make_responsive(partial(start_pull_process, uow=uow)), output_port=operation_presenter
        )
        start_delete_process_service = partial(
            make_responsive(partial(start_delete_process, uow=uow)), output_port=operation_presenter
        )
        process_to_completion_service = partial(
            make_responsive(partial(process_to_completion, process_service=process_service)), output_port=lambda x: None
        )
        handlers = {
            Services.PULL: partial(
                pull,
                process_to_completion_service=process_to_completion_service,
                start_pull_process_service=start_pull_process_service,
                output_port=lambda x: None,
            ),
            Services.DELETE: partial(
                delete,
                process_to_completion_service=process_to_completion_service,
                start_delete_process_service=start_delete_process_service,
                output_port=lambda x: None,
            ),
            Services.PROCESS: partial(process, uow=uow, output_port=operation_presenter),
            Services.LIST_IDLE_ENTITIES: partial(list_idle_entities, uow=uow, output_port=idle_entities_updater),
        }
        controller = DJController(handlers, translator)
        source_restriction.callback = controller.list_idle_entities

        return create_local_endpoint(controller, tables, source_restriction)

    return inner
