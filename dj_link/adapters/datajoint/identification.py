"""Contains code used to translate between DataJoint primary keys and identifiers."""
import hashlib
import json

from ...base import Base
from ...types import PrimaryKey
from .abstract_facade import AbstractTableFacade


class IdentificationTranslator(Base):
    """Used to translate between DataJoint primary keys and identifiers."""

    def __init__(self, table_facade: AbstractTableFacade) -> None:
        """Initialize the identification translator."""
        self.table_facade = table_facade

    @staticmethod
    def to_identifier(primary_key: PrimaryKey) -> str:
        """Translate the provided primary key to its corresponding identifier."""
        return hashlib.sha1(json.dumps(primary_key, sort_keys=True).encode()).hexdigest()

    def to_primary_key(self, identifier: str) -> PrimaryKey:
        """Translate the provided identifier to its corresponding primary key."""
        mapping = {self.to_identifier(primary_key): primary_key for primary_key in self.table_facade.primary_keys}
        return mapping[identifier]
