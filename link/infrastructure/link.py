"""Contains the link decorator that is used by the user to establish a link."""
from __future__ import annotations

import logging
from collections.abc import Callable
from functools import partial
from typing import Any, Mapping, Optional

from link.adapters.controller import DJController
from link.adapters.gateway import DJLinkGateway
from link.adapters.identification import IdentificationTranslator
from link.adapters.present import create_state_change_logger
from link.adapters.progress import DJProgressDisplayAdapter
from link.domain import commands, events
from link.service.handlers import (
    delete,
    delete_entity,
    inform_batch_processing_finished,
    inform_batch_processing_started,
    inform_current_process_finished,
    inform_next_process_started,
    log_state_change,
    pull,
    pull_entity,
)
from link.service.messagebus import CommandHandlers, EventHandlers, MessageBus
from link.service.uow import UnitOfWork

from . import DJConfiguration, create_tables
from .facade import DJLinkFacade
from .mixin import create_local_endpoint
from .progress import TQDMProgressView


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
        logger = logging.getLogger(obj.__name__)

        command_handlers: CommandHandlers = {}
        event_handlers: EventHandlers = {}
        bus = MessageBus(uow, command_handlers, event_handlers)
        command_handlers[commands.PullEntity] = partial(pull_entity, uow=uow, message_bus=bus)
        command_handlers[commands.DeleteEntity] = partial(delete_entity, uow=uow, message_bus=bus)
        command_handlers[commands.PullEntities] = partial(pull, message_bus=bus)
        command_handlers[commands.DeleteEntities] = partial(delete, message_bus=bus)
        progress_view = TQDMProgressView()
        display = DJProgressDisplayAdapter(translator, progress_view)
        event_handlers[events.ProcessStarted] = [partial(inform_next_process_started, display=display)]
        event_handlers[events.ProcessFinished] = [partial(inform_current_process_finished, display=display)]
        event_handlers[events.BatchProcessingStarted] = [partial(inform_batch_processing_started, display=display)]
        event_handlers[events.BatchProcessingFinished] = [partial(inform_batch_processing_finished, display=display)]
        event_handlers[events.StateChanged] = [
            partial(log_state_change, log=create_state_change_logger(translator, logger.info))
        ]
        event_handlers[events.InvalidOperationRequested] = [lambda event: None]

        controller = DJController(bus, translator)

        return create_local_endpoint(controller, tables, progress_view)

    return inner
