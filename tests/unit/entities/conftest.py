from unittest.mock import MagicMock
from typing import Any

import pytest

from link.entities.repository import Entity
from link.entities.gateway import AbstractGateway


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def identifier(identifiers):
    return identifiers[0]


@pytest.fixture
def entities(identifiers):
    return {identifier: Entity(identifier) for identifier in identifiers}


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
def gateway_spy(wrap_spy_around_method, entity_data):
    class GatewaySpy(AbstractGateway):
        def __init__(self):
            self.in_transaction = False
            self.error_when_starting = False
            self.error_when_committing = False
            self.error_when_cancelling = False

        def fetch(self, identifier: str) -> Any:
            return entity_data

        def insert(self, identifier: str, data: Any) -> None:
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

        def __repr__(self) -> str:
            return "gateway_spy"

    gateway_spy = GatewaySpy()
    for method in [
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
