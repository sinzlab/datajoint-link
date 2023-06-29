"""Contains code used to translate between DataJoint primary keys and identifiers."""
from __future__ import annotations

from collections.abc import Iterable
from typing import Union
from uuid import UUID, uuid4

from dj_link.entities.custom_types import Identifier

from ...custom_types import PrimaryKey


class IdentificationTranslator:
    """Translates between DataJoint-specific primary keys and domain-model-specific identifiers."""

    def __init__(self) -> None:
        """Initialize the translator."""
        self.__mapping: dict[tuple[tuple[str, Union[str, int, float]], ...], UUID] = {}

    def to_identifier(self, primary_key: PrimaryKey) -> Identifier:
        """Translate the given primary key to its corresponding identifier."""
        primary_key_tuple = tuple((k, v) for k, v in primary_key.items())
        return Identifier(self.__mapping.setdefault(primary_key_tuple, uuid4()))

    def to_identifiers(self, primary_keys: Iterable[PrimaryKey]) -> set[Identifier]:
        """Translate multiple primary keys to their corresponding identifiers."""
        return {self.to_identifier(key) for key in primary_keys}

    def to_primary_key(self, identifier: Identifier) -> PrimaryKey:
        """Translate the given identifier to its corresponding primary key."""
        primary_key_tuple = {v: k for k, v in self.__mapping.items()}[identifier]
        return dict(primary_key_tuple)
