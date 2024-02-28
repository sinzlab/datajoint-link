"""Contains the DataJoint gateway class and related classes/functions."""
from __future__ import annotations

from itertools import groupby
from typing import Iterable

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.link import create_entity
from link.domain.state import Commands, Components, Entity, Processes
from link.service.gateway import LinkGateway

from .facade import DJLinkFacade
from .identification import IdentificationTranslator


class DJLinkGateway(LinkGateway):
    """Gateway for links stored using DataJoint."""

    def __init__(self, facade: DJLinkFacade, translator: IdentificationTranslator) -> None:
        """Initialize the gateway."""
        self.facade = facade
        self.translator = translator

    def create_entity(self, identifier: Identifier) -> Entity:
        """Create a entity instance from persistent data."""
        dj_assignment = self.facade.get_assignment(self.translator.to_primary_key(identifier))
        components = []
        if dj_assignment.source:
            components.append(Components.SOURCE)
        if dj_assignment.outbound:
            components.append(Components.OUTBOUND)
        if dj_assignment.local:
            components.append(Components.LOCAL)
        dj_condition = self.facade.get_condition(self.translator.to_primary_key(identifier))
        persisted_to_domain_process_map = {"PULL": Processes.PULL, "DELETE": Processes.DELETE, "NONE": Processes.NONE}
        dj_process = self.facade.get_process(self.translator.to_primary_key(identifier))
        return create_entity(
            identifier,
            components=components,
            is_tainted=dj_condition.is_flagged,
            process=persisted_to_domain_process_map[dj_process.current_process],
        )

    def apply(self, updates: Iterable[events.StateChanged]) -> None:
        """Apply updates to the persistent data representing the link."""

        def keyfunc(update: events.StateChanged) -> int:
            assert update.command is not None
            return update.command.value

        transition_updates = (update for update in updates if update.command)
        for command_value, command_updates in groupby(sorted(transition_updates, key=keyfunc), key=keyfunc):
            primary_keys = (self.translator.to_primary_key(update.identifier) for update in command_updates)
            if Commands(command_value) is Commands.ADD_TO_LOCAL:
                self.facade.add_to_local(primary_keys)
            if Commands(command_value) is Commands.REMOVE_FROM_LOCAL:
                self.facade.remove_from_local(primary_keys)
            if Commands(command_value) is Commands.START_PULL_PROCESS:
                self.facade.start_pull_process(primary_keys)
            if Commands(command_value) is Commands.FINISH_PULL_PROCESS:
                self.facade.finish_pull_process(primary_keys)
            if Commands(command_value) is Commands.DEPRECATE:
                self.facade.deprecate(primary_keys)
            if Commands(command_value) is Commands.START_DELETE_PROCESS:
                self.facade.start_delete_process(primary_keys)
            if Commands(command_value) is Commands.FINISH_DELETE_PROCESS:
                self.facade.finish_delete_process(primary_keys)
