"""Contains the link decorator that is used by the user to establish a link."""
from __future__ import annotations

import logging
from collections.abc import Callable
from functools import partial
from typing import Any, Mapping, Optional

from link.adapters.controller import DJController
from link.adapters.custom_types import PrimaryKey
from link.adapters.gateway import DJLinkGateway
from link.adapters.identification import IdentificationTranslator
from link.adapters.present import create_idle_entities_updater, create_state_change_logger
from link.domain import commands, events
from link.service.handlers import delete, delete_entity, list_idle_entities, log_state_change, pull, pull_entity
from link.service.messagebus import CommandHandlers, EventHandlers, MessageBus
from link.service.uow import UnitOfWork

from . import DJConfiguration, create_tables
from .facade import DJLinkFacade
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
        logger = logging.getLogger(obj.__name__)

        command_handlers: CommandHandlers = {}
        event_handlers: EventHandlers = {}
        bus = MessageBus(uow, command_handlers, event_handlers)
        command_handlers[commands.PullEntity] = partial(pull_entity, uow=uow, message_bus=bus)
        command_handlers[commands.DeleteEntity] = partial(delete_entity, uow=uow, message_bus=bus)
        command_handlers[commands.PullEntities] = partial(pull, message_bus=bus)
        command_handlers[commands.DeleteEntities] = partial(delete, message_bus=bus)
        command_handlers[commands.ListIdleEntities] = partial(
            list_idle_entities, uow=uow, output_port=idle_entities_updater
        )
        event_handlers[events.ProcessStarted] = [lambda event: None]
        event_handlers[events.ProcessFinished] = [lambda event: None]
        event_handlers[events.ProcessesStarted] = [lambda event: None]
        event_handlers[events.ProcessesFinished] = [lambda event: None]
        event_handlers[events.StateChanged] = [
            partial(log_state_change, log=create_state_change_logger(translator, logger.info))
        ]
        event_handlers[events.InvalidOperationRequested] = [lambda event: None]

        controller = DJController(bus, translator)
        source_restriction.callback = controller.list_idle_entities

        return create_local_endpoint(controller, tables, source_restriction)

    return inner
