"""Contains custom types."""
from typing import NewType
from uuid import UUID

Identifier = NewType("Identifier", UUID)
