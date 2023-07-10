"""Contains the DataJoint gateway class and related classes/functions."""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from itertools import groupby, tee
from typing import Any, Dict, Iterable, Iterator, List

from dj_link.custom_types import PrimaryKey
from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import Link, create_link
from dj_link.entities.state import Commands, Processes, Update
from dj_link.use_cases.gateway import LinkGateway

from ...base import Base
from ...entities.abstract_gateway import AbstractEntityDTO, AbstractGateway
from ...entities.link import Transfer
from ...entities.state import Components
from ...use_cases.gateway import GatewayLink
from .abstract_facade import AbstractTableFacade
from .facade import DJAssignments, DJLinkFacade, DJProcess
from .identification import IdentificationTranslator


@dataclass
class EntityDTO(AbstractEntityDTO):
    """Data transfer object representing a entity."""

    primary_key: List[str]
    master: Dict[str, Any]
    parts: Dict[str, Any] = field(default_factory=dict)

    def create_identifier_only_copy(self) -> EntityDTO:
        """Create a new instance of the class containing only the data used to compute the identifier."""
        # noinspection PyArgumentList
        return self.__class__(self.primary_key, {k: v for k, v in self.master.items() if k in self.primary_key})


class DataJointGateway(AbstractGateway[EntityDTO], Base):
    """Gateway between the entities/use-cases and the DataJoint table facade."""

    def __init__(self, table_facade: AbstractTableFacade, translator: IdentificationTranslator) -> None:
        """Initialize the DataJoint gateway."""
        self.table_facade = table_facade
        self.translator = translator

    def get_identifiers_in_restriction(self, restriction) -> List[Identifier]:
        """Return the identifiers of all entities in the provided restriction."""
        primary_keys = self.table_facade.get_primary_keys_in_restriction(restriction)
        return [self.translator.to_identifier(primary_key) for primary_key in primary_keys]

    def get_flags(self, identifier: Identifier) -> Dict[str, bool]:
        """Get the names and values of all flags that are set on the entity identified by the provided identifier."""
        flags = self.table_facade.get_flags(self.translator.to_primary_key(identifier))
        return {self.to_flag_name(flag_table_name): flag for flag_table_name, flag in flags.items()}

    def fetch(self, identifier: Identifier) -> EntityDTO:
        """Fetch the entity identified by the provided identifier.

        Raises a key error if the entity is missing.
        """
        primary_key = self.translator.to_primary_key(identifier)
        try:
            return self.table_facade.fetch(primary_key)
        except KeyError as exc:
            raise KeyError(identifier) from exc

    def insert(self, entity_dto: EntityDTO) -> None:
        """Insert the provided entity into the table."""
        self.table_facade.insert(entity_dto)

    def delete(self, identifier: Identifier) -> None:
        """Delete the entity identified by the provided identifier from the table."""
        primary_key = self.translator.to_primary_key(identifier)
        self.table_facade.delete(primary_key)

    def set_flag(self, identifier: Identifier, flag: str, value: bool) -> None:
        """Set the flag on the entity identified by the provided identifier to the provided value."""
        if value:
            self._enable_flag(identifier, flag)
        else:
            self._disable_flag(identifier, flag)

    def _enable_flag(self, identifier: Identifier, flag: str) -> None:
        self.table_facade.enable_flag(self.translator.to_primary_key(identifier), self._to_flag_table_name(flag))

    def _disable_flag(self, identifier: Identifier, flag: str) -> None:
        self.table_facade.disable_flag(self.translator.to_primary_key(identifier), self._to_flag_table_name(flag))

    def start_transaction(self) -> None:
        """Start a transaction in the table."""
        self.table_facade.start_transaction()

    def commit_transaction(self) -> None:
        """Commit a transaction in the table."""
        self.table_facade.commit_transaction()

    def cancel_transaction(self) -> None:
        """Cancel a transaction in the table."""
        self.table_facade.cancel_transaction()

    @staticmethod
    def to_flag_name(flag_table_name: str) -> str:
        """Translate the provided flag table name to the corresponding flag name."""
        indexes = [index for index, letter in enumerate(flag_table_name) if letter.isupper() and index != 0]
        starts, stops = tee(indexes + [len(flag_table_name)])
        next(stops, None)
        parts = ["_" + flag_table_name[start:stop] for start, stop in zip(starts, stops)]
        return "".join([flag_table_name[: indexes[0]]] + parts).lower()

    @staticmethod
    def _to_flag_table_name(flag_name: str) -> str:
        """Translate the provided flag name to the corresponding flag table name."""
        return "".join(part.title() for part in flag_name.split("_"))

    def __len__(self) -> int:
        """Return the number of entities in the corresponding table."""
        return len(self.table_facade)

    def __iter__(self) -> Iterator[Identifier]:
        """Iterate over all identifiers in the table."""
        for primary_key in self.table_facade:
            yield self.translator.to_identifier(primary_key)


class DataJointGatewayLink(GatewayLink, Base):
    """Contains the three DataJoint gateways corresponding to the three table types."""

    def __init__(self, source: DataJointGateway, outbound: DataJointGateway, local: DataJointGateway):
        """Initialize the DataJoint gateway link."""
        self._source = source
        self._outbound = outbound
        self._local = local

    @property
    def source(self) -> DataJointGateway:
        """Return the source gateway."""
        return self._source

    @property
    def outbound(self) -> DataJointGateway:
        """Return the outbound gateway."""
        return self._outbound

    @property
    def local(self) -> DataJointGateway:
        """Return the local gateway."""
        return self._local

    def transfer(self, spec: Transfer) -> None:
        """Transfer an entity from one table in the link to another."""
        gateway_map = {
            Components.SOURCE: self._source,
            Components.OUTBOUND: self._outbound,
            Components.LOCAL: self._local,
        }
        origin = gateway_map[spec.origin]
        destination = gateway_map[spec.destination]
        entity = origin.fetch(spec.identifier)
        if spec.identifier_only:
            entity = entity.create_identifier_only_copy()
        destination.insert(entity)


class DJLinkGateway(LinkGateway):
    """Gateway for links stored using DataJoint."""

    def __init__(self, facade: DJLinkFacade, translator: IdentificationTranslator) -> None:
        """Initialize the gateway."""
        self.facade = facade
        self.translator = translator

    def create_link(self) -> Link:
        """Create a link instance from persistent data."""

        def translate_assignments(dj_assignments: DJAssignments) -> dict[Components, set[Identifier]]:
            return {
                Components.SOURCE: self.translator.to_identifiers(dj_assignments.source),
                Components.OUTBOUND: self.translator.to_identifiers(dj_assignments.outbound),
                Components.LOCAL: self.translator.to_identifiers(dj_assignments.local),
            }

        def translate_processes(dj_processes: Iterable[DJProcess]) -> dict[Processes, set[Identifier]]:
            persisted_to_domain_process_map = {"PULL": Processes.PULL, "DELETE": Processes.DELETE}
            domain_processes: dict[Processes, set[Identifier]] = defaultdict(set)
            active_processes = [process for process in dj_processes if process.current_process != "NONE"]
            for persisted_process in active_processes:
                domain_process = persisted_to_domain_process_map[persisted_process.current_process]
                domain_processes[domain_process].add(self.translator.to_identifier(persisted_process.primary_key))
            return domain_processes

        def translate_tainted_primary_keys(primary_keys: Iterable[PrimaryKey]) -> set[Identifier]:
            return {self.translator.to_identifier(key) for key in primary_keys}

        return create_link(
            translate_assignments(self.facade.get_assignments()),
            processes=translate_processes(self.facade.get_processes()),
            tainted_identifiers=translate_tainted_primary_keys(self.facade.get_tainted_primary_keys()),
        )

    def apply(self, updates: Iterable[Update]) -> None:
        """Apply updates to the persistent data representing the link."""

        def keyfunc(update: Update) -> int:
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
