from unittest.mock import MagicMock, create_autospec
from typing import Any, List, Dict

import pytest

from link.entities.repository import Entity
from link.entities.abstract_gateway import AbstractGateway


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def identifier(identifiers):
    return identifiers[0]


@pytest.fixture
def flags(identifiers):
    return {identifier: dict(flag1=True, flag2=False) for identifier in identifiers}


@pytest.fixture
def entities(flags):
    return {
        identifier: create_autospec(Entity, instance=True, identifier=identifier, flags=entity_flags)
        for identifier, entity_flags in flags.items()
    }


@pytest.fixture
def entity(identifier, entities):
    return entities[identifier]


@pytest.fixture
def entity_data():
    return "data"


@pytest.fixture
def wrap_spy_around_method():
    def _wrap_spy_around_method(instance, method):
        setattr(
            instance,
            method,
            MagicMock(name=instance.__class__.__name__ + "." + method, wraps=getattr(instance, method)),
        )

    return _wrap_spy_around_method


@pytest.fixture
def gateway_spy_cls():
    class GatewaySpy(AbstractGateway):
        def __init__(self, identifiers, flags, entity_data):
            self._identifiers = identifiers
            self.flags = flags
            self.entity_data = entity_data
            self.in_transaction = False
            self.error_when_starting = False
            self.error_when_committing = False
            self.error_when_cancelling = False

        @property
        def identifiers(self) -> List[str]:
            return self._identifiers

        def get_flags(self, identifier: str) -> Dict[str, bool]:
            return self.flags[identifier]

        def fetch(self, identifier: str) -> Any:
            return self.entity_data

        def insert(self, data: Any) -> None:
            pass

        def delete(self, identifier: str) -> None:
            pass

        def set_flag(self, identifier: str, flag: str, value: bool) -> None:
            pass

        def start_transaction(self) -> None:
            if self.in_transaction:
                raise Exception
            if self.error_when_starting:
                raise RuntimeError
            self.in_transaction = True

        def commit_transaction(self) -> None:
            if not self.in_transaction:
                raise Exception
            if self.error_when_committing:
                raise RuntimeError
            self.in_transaction = False

        def cancel_transaction(self) -> None:
            if not self.in_transaction:
                raise Exception
            if self.error_when_cancelling:
                raise RuntimeError
            self.in_transaction = False

    return GatewaySpy


@pytest.fixture
def gateway_spy(gateway_spy_cls, identifiers, flags, entity_data, wrap_spy_around_method):
    gateway_spy = gateway_spy_cls(identifiers, flags, entity_data)
    for method in [
        "get_flags",
        "fetch",
        "insert",
        "delete",
        "set_flag",
        "start_transaction",
        "commit_transaction",
        "cancel_transaction",
    ]:
        wrap_spy_around_method(gateway_spy, method)
    return gateway_spy
