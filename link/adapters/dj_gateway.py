import json
import hashlib
from typing import List, Dict, Any
from functools import wraps

from . import gateway


def _identifiers_to_primary_keys(method):
    @wraps(method)
    def wrapper(self, identifiers):
        primary_keys = [self._identifiers_to_primary_keys_mapping[identifier] for identifier in identifiers]
        return method(self, primary_keys)

    return wrapper


class SourceGateway(gateway.AbstractSourceGateway):
    def __init__(self, table_proxy):
        self.table_proxy = table_proxy

    @property
    def identifiers(self) -> List[str]:
        return [self._hash_primary_key(key) for key in self.table_proxy.primary_keys]

    @_identifiers_to_primary_keys
    def fetch(self, identifiers: List[str]) -> Dict[str, Any]:
        data = dict()
        for entity in self.table_proxy.fetch(identifiers):
            primary_key = {pan: entity.pop(pan) for pan in self.table_proxy.primary_attr_names}
            data[self._hash_primary_key(primary_key)] = entity
        return data

    @property
    def _identifiers_to_primary_keys_mapping(self):
        return {self._hash_primary_key(key): key for key in self.table_proxy.primary_keys}

    @staticmethod
    def _hash_primary_key(primary_key):
        return hashlib.sha1(json.dumps(primary_key, sort_keys=True).encode()).hexdigest()

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.table_proxy})"


class NonSourceGateway(SourceGateway, gateway.AbstractNonSourceGateway):
    @property
    def deletion_requested_identifiers(self) -> List[str]:
        return [self._hash_primary_key(key) for key in self.table_proxy.deletion_requested]

    @_identifiers_to_primary_keys
    def delete(self, identifiers: List[str]) -> None:
        self.table_proxy.delete(identifiers)

    @_identifiers_to_primary_keys
    def insert(self, identifiers: List[str]) -> None:
        self.table_proxy.insert(identifiers)

    def start_transaction(self) -> None:
        self.table_proxy.start_transaction()

    def commit_transaction(self) -> None:
        self.table_proxy.commit_transaction()

    def cancel_transaction(self) -> None:
        self.table_proxy.cancel_transaction()


class OutboundGateway(NonSourceGateway, gateway.AbstractOutboundGateway):
    @property
    def deletion_approved_identifiers(self) -> List[str]:
        return [self._hash_primary_key(key) for key in self.table_proxy.deletion_approved]

    @_identifiers_to_primary_keys
    def approve_deletion(self, identifiers: List[str]) -> None:
        self.table_proxy.approve_deletion(identifiers)


class LocalGateway(NonSourceGateway, gateway.AbstractLocalGateway):
    pass
