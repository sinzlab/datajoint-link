import hashlib
import json

from ...types import PrimaryKey
from .proxy import AbstractTableProxy
from ...entities.representation import represent


class IdentificationTranslator:
    def __init__(self, table_proxy: AbstractTableProxy) -> None:
        self.table_proxy = table_proxy

    @staticmethod
    def to_identifier(primary_key: PrimaryKey) -> str:
        """Translates the provided primary key to its corresponding identifier."""
        return hashlib.sha1(json.dumps(primary_key, sort_keys=True).encode()).hexdigest()

    def to_primary_key(self, identifier: str) -> PrimaryKey:
        """Translates the provided identifier to its corresponding primary key."""
        mapping = {self.to_identifier(primary_key): primary_key for primary_key in self.table_proxy.primary_keys}
        return mapping[identifier]

    def __repr__(self) -> str:
        return represent(self, ["table_proxy"])
