from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, List, Dict

from ...types import PrimaryKey

if TYPE_CHECKING:
    from .gateway import EntityDTO


class AbstractTableFacade(ABC):
    @property
    @abstractmethod
    def primary_keys(self) -> List[PrimaryKey]:
        pass

    @abstractmethod
    def get_primary_keys_in_restriction(self, restriction) -> List[PrimaryKey]:
        pass

    @abstractmethod
    def get_flags(self, primary_key: PrimaryKey) -> Dict[str, bool]:
        pass

    @abstractmethod
    def fetch(self, primary_key: PrimaryKey) -> EntityDTO:
        pass

    @abstractmethod
    def insert(self, entity_dto: EntityDTO) -> None:
        pass

    @abstractmethod
    def delete(self, primary_key: PrimaryKey) -> None:
        pass

    @abstractmethod
    def enable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        pass

    @abstractmethod
    def disable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        pass

    @abstractmethod
    def start_transaction(self) -> None:
        pass

    @abstractmethod
    def commit_transaction(self) -> None:
        pass

    @abstractmethod
    def cancel_transaction(self) -> None:
        pass
