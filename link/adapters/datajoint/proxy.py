from abc import ABC, abstractmethod
from typing import List, Dict, Any

from ...types import PrimaryKey


class AbstractTableProxy(ABC):
    @property
    @abstractmethod
    def primary_keys(self) -> List[PrimaryKey]:
        pass

    @abstractmethod
    def get_flags(self, primary_key: PrimaryKey) -> Dict[str, bool]:
        pass

    @abstractmethod
    def fetch_master(self, primary_key: PrimaryKey) -> Dict[str, Any]:
        pass

    @abstractmethod
    def fetch_parts(self, primary_key: PrimaryKey) -> Dict[str, Any]:
        pass

    @abstractmethod
    def insert_master(self, master_entity: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def insert_parts(self, part_entities: Dict[str, Any]) -> None:
        pass

    @abstractmethod
    def delete_master(self, primary_key: PrimaryKey) -> None:
        pass

    @abstractmethod
    def delete_parts(self, primary_key: PrimaryKey) -> None:
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
