from __future__ import annotations

from collections.abc import Mapping
from typing import Iterable

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.link import Link, create_entity, create_link
from link.domain.state import Commands, Components, Entity, Processes
from link.service.gateway import LinkGateway


class FakeLinkGateway(LinkGateway):
    def __init__(
        self,
        assignments: Mapping[Components, Iterable[Identifier]],
        *,
        tainted_identifiers: Iterable[Identifier] | None = None,
        processes: Mapping[Processes, Iterable[Identifier]] | None = None,
    ) -> None:
        self.assignments = {component: set(identifiers) for component, identifiers in assignments.items()}
        self.tainted_identifiers = set(tainted_identifiers) if tainted_identifiers is not None else set()
        self.processes: dict[Processes, set[Identifier]] = {process: set() for process in Processes}
        if processes is not None:
            for entity_process, identifiers in processes.items():
                self.processes[entity_process].update(identifiers)

    def create_link(self) -> Link:
        return create_link(self.assignments, tainted_identifiers=self.tainted_identifiers, processes=self.processes)

    def create_entity(self, identifier: Identifier) -> Entity:
        assignment = (component for component in self.assignments if identifier in self.assignments[component])
        try:
            process = next(process for process in self.processes if identifier in self.processes[process])
        except StopIteration:
            process = Processes.NONE
        return create_entity(
            identifier,
            components=assignment,
            is_tainted=identifier in self.tainted_identifiers,
            process=process,
        )

    def apply(self, updates: Iterable[events.StateChanged]) -> None:
        for update in updates:
            if update.command is Commands.START_PULL_PROCESS:
                self.processes[Processes.PULL].add(update.identifier)
                self.assignments[Components.OUTBOUND].add(update.identifier)
            elif update.command is Commands.ADD_TO_LOCAL:
                self.assignments[Components.LOCAL].add(update.identifier)
            elif update.command is Commands.FINISH_PULL_PROCESS:
                self.processes[Processes.PULL].remove(update.identifier)
            elif update.command is Commands.START_DELETE_PROCESS:
                self.processes[Processes.DELETE].add(update.identifier)
            elif update.command is Commands.REMOVE_FROM_LOCAL:
                self.assignments[Components.LOCAL].remove(update.identifier)
            elif update.command is Commands.FINISH_DELETE_PROCESS:
                self.processes[Processes.DELETE].remove(update.identifier)
                self.assignments[Components.OUTBOUND].remove(update.identifier)
            elif update.command is Commands.DEPRECATE:
                try:
                    self.processes[Processes.DELETE].remove(update.identifier)
                except KeyError:
                    self.processes[Processes.PULL].remove(update.identifier)
            else:
                raise ValueError("Unsupported command encountered")
