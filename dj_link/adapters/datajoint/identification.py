"""Contains code used to translate between DataJoint primary keys and identifiers."""
import hashlib
import json
from typing import Dict

from ...base import Base
from ...custom_types import PrimaryKey


class IdentificationTranslator(Base):
    """Used to translate between DataJoint primary keys and identifiers."""

    def __init__(self) -> None:
        """Initialize the identification translator."""
        self._identifier_to_primary_key_mapping: Dict[str, PrimaryKey] = dict()

    def to_identifier(self, primary_key: PrimaryKey) -> str:
        """Translate the provided primary key to its corresponding identifier."""
        identifier = hashlib.blake2b(json.dumps(primary_key, sort_keys=True).encode()).hexdigest()
        self._identifier_to_primary_key_mapping[identifier] = primary_key
        return identifier

    def to_primary_key(self, identifier: str) -> PrimaryKey:
        """Translate the provided identifier to its corresponding primary key."""
        return self._identifier_to_primary_key_mapping[identifier]
