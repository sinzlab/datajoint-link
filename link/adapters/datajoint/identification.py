import hashlib
import json

from ...types import PrimaryKey
from .abstract_facade import AbstractTableFacade
from ...base import Base


class IdentificationTranslator(Base):
    def __init__(self, table_facade: AbstractTableFacade) -> None:
        self.table_facade = table_facade

    @staticmethod
    def to_identifier(primary_key: PrimaryKey) -> str:
        """Translates the provided primary key to its corresponding identifier."""
        return hashlib.sha1(json.dumps(primary_key, sort_keys=True).encode()).hexdigest()

    def to_primary_key(self, identifier: str) -> PrimaryKey:
        """Translates the provided identifier to its corresponding primary key."""
        mapping = {self.to_identifier(primary_key): primary_key for primary_key in self.table_facade.primary_keys}
        return mapping[identifier]
