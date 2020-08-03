from abc import ABC, abstractmethod
from typing import List, Dict, Any

from ...types import PrimaryKey


class AbstractTableEntityDTO(ABC):
    @property
    @abstractmethod
    def primary_key(self) -> PrimaryKey:
        pass

    @property
    @abstractmethod
    def master_entity(self) -> Dict[str, Any]:
        pass

    @property
    @abstractmethod
    def part_entities(self) -> Dict[str, Any]:
        pass


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
    def fetch(self, primary_key: PrimaryKey) -> AbstractTableEntityDTO:
        pass

    @abstractmethod
    def insert(self, entity_dto: AbstractTableEntityDTO) -> None:
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
