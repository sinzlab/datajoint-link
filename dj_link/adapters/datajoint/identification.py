"""Contains code used to translate between DataJoint primary keys and identifiers."""
from __future__ import annotations

import hashlib
import json
from typing import Dict, Union
from uuid import UUID, uuid4

from dj_link.entities.custom_types import Identifier

from ...base import Base
from ...custom_types import PrimaryKey


class IdentificationTranslator(Base):
    """Used to translate between DataJoint primary keys and identifiers."""

    def __init__(self) -> None:
        """Initialize the identification translator."""
        self._identifier_to_primary_key_mapping: Dict[str, PrimaryKey] = {}

    def to_identifier(self, primary_key: PrimaryKey) -> str:
        """Translate the provided primary key to its corresponding identifier."""
        identifier = hashlib.blake2b(json.dumps(primary_key, sort_keys=True).encode()).hexdigest()
        self._identifier_to_primary_key_mapping[identifier] = primary_key
        return identifier

    def to_primary_key(self, identifier: str) -> PrimaryKey:
        """Translate the provided identifier to its corresponding primary key."""
        return self._identifier_to_primary_key_mapping[identifier]


class UUIDIdentificationTranslator:
    """Translates between DataJoint-specific primary keys and domain-model-specific identifiers."""

    def __init__(self) -> None:
        """Initialize the translator."""
        self.__mapping: dict[tuple[tuple[str, Union[str, int, float]], ...], UUID] = {}

    def to_identifier(self, primary_key: PrimaryKey) -> Identifier:
        """Translate the given primary key to its corresponding identifier."""
        primary_key_tuple = tuple((k, v) for k, v in primary_key.items())
        return Identifier(self.__mapping.setdefault(primary_key_tuple, uuid4()))

    def to_primary_key(self, identifier: Identifier) -> PrimaryKey:
        """Translate the given identifier to its corresponding primary key."""
        primary_key_tuple = {v: k for k, v in self.__mapping.items()}[identifier]
        return dict(primary_key_tuple)
