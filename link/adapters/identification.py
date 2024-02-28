"""Contains code used to translate between DataJoint primary keys and identifiers."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Union
from uuid import uuid4

from link.domain.custom_types import Identifier

from .custom_types import PrimaryKey


class IdentificationTranslator:
    """Translates between DataJoint-specific primary keys and domain-model-specific identifiers."""

    def __init__(self) -> None:
        """Initialize the translator."""
        self._key_to_identifier: dict[tuple[tuple[str, Union[str, int, float]], ...], Identifier] = {}
        self._identifier_to_key: dict[Identifier, tuple[tuple[str, Union[str, int, float]], ...]] = {}

    def to_identifier(self, primary_key: PrimaryKey) -> Identifier:
        """Translate the given primary key to its corresponding identifier."""
        primary_key_tuple = tuple((k, v) for k, v in primary_key.items())
        identifier = self._key_to_identifier.setdefault(primary_key_tuple, Identifier(uuid4()))
        self._identifier_to_key[identifier] = primary_key_tuple
        return identifier

    def to_identifiers(self, primary_keys: Iterable[PrimaryKey]) -> set[Identifier]:
        """Translate multiple primary keys to their corresponding identifiers."""
        return {self.to_identifier(key) for key in primary_keys}

    def to_primary_key(self, identifier: Identifier) -> PrimaryKey:
        """Translate the given identifier to its corresponding primary key."""
        return dict(self._identifier_to_key[identifier])
