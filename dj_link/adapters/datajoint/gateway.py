from __future__ import annotations
from typing import List, Dict, Any
from itertools import tee
from dataclasses import dataclass, field

from .abstract_facade import AbstractTableFacade
from .identification import IdentificationTranslator
from ...entities.abstract_gateway import AbstractEntityDTO, AbstractGateway
from ...base import Base


@dataclass
class EntityDTO(AbstractEntityDTO):
    """Data transfer object representing a entity."""

    primary_key: List[str]
    master: Dict[str, Any]
    parts: Dict[str, Any] = field(default_factory=dict)

    def create_identifier_only_copy(self) -> EntityDTO:
        """Creates a new instance of the class containing only the data used to compute the identifier."""
        # noinspection PyArgumentList
        return self.__class__(self.primary_key, {k: v for k, v in self.master.items() if k in self.primary_key})


class DataJointGateway(AbstractGateway[EntityDTO], Base):
    def __init__(self, table_facade: AbstractTableFacade, translator: IdentificationTranslator) -> None:
        self.table_facade = table_facade
        self.translator = translator

    @property
    def identifiers(self) -> List[str]:
        """Returns the identifiers of all entities in the table."""
        return [self.translator.to_identifier(primary_key) for primary_key in self.table_facade.primary_keys]

    def get_identifiers_in_restriction(self, restriction) -> List[str]:
        """Returns the identifiers of all entities in the provided restriction."""
        primary_keys = self.table_facade.get_primary_keys_in_restriction(restriction)
        return [self.translator.to_identifier(primary_key) for primary_key in primary_keys]

    def get_flags(self, identifier: str) -> Dict[str, bool]:
        """Gets the names and values of all flags that are set on the entity identified by the provided identifier."""
        flags = self.table_facade.get_flags(self.translator.to_primary_key(identifier))
        return {self.to_flag_name(flag_table_name): flag for flag_table_name, flag in flags.items()}

    def fetch(self, identifier: str) -> EntityDTO:
        """Fetches the entity identified by the provided identifier."""
        primary_key = self.translator.to_primary_key(identifier)
        return self.table_facade.fetch(primary_key)

    def insert(self, entity_dto: EntityDTO) -> None:
        """Inserts the provided entity into the table."""
        self.table_facade.insert(entity_dto)

    def delete(self, identifier: str) -> None:
        """Deletes the entity identified by the provided identifier from the table."""
        primary_key = self.translator.to_primary_key(identifier)
        self.table_facade.delete(primary_key)

    def set_flag(self, identifier: str, flag: str, value: bool) -> None:
        """Sets the flag on the entity identified by the provided identifier to the provided value."""
        if value:
            self._enable_flag(identifier, flag)
        else:
            self._disable_flag(identifier, flag)

    def _enable_flag(self, identifier: str, flag: str) -> None:
        self.table_facade.enable_flag(self.translator.to_primary_key(identifier), self._to_flag_table_name(flag))

    def _disable_flag(self, identifier: str, flag: str) -> None:
        self.table_facade.disable_flag(self.translator.to_primary_key(identifier), self._to_flag_table_name(flag))

    def start_transaction(self) -> None:
        """Starts a transaction in the table."""
        self.table_facade.start_transaction()

    def commit_transaction(self) -> None:
        """Commits a transaction in the table."""
        self.table_facade.commit_transaction()

    def cancel_transaction(self) -> None:
        """Cancels a transaction in the table."""
        self.table_facade.cancel_transaction()

    @staticmethod
    def to_flag_name(flag_table_name: str) -> str:
        """Translates the provided flag table name to the corresponding flag name."""
        indexes = [index for index, letter in enumerate(flag_table_name) if letter.isupper() and index != 0]
        starts, stops = tee(indexes + [len(flag_table_name)])
        next(stops, None)
        parts = ["_" + flag_table_name[start:stop] for start, stop in zip(starts, stops)]
        return "".join([flag_table_name[: indexes[0]]] + parts).lower()

    @staticmethod
    def _to_flag_table_name(flag_name: str) -> str:
        """Translates the provided flag name to the corresponding flag table name."""
        return "".join(part.title() for part in flag_name.split("_"))
