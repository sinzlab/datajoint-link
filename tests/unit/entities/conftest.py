from unittest.mock import MagicMock

import pytest


@pytest.fixture
def address():
    address = MagicMock(name="address")
    address.__repr__ = MagicMock(return_value="address")
    return address


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(10)]


@pytest.fixture
def entities(identifiers):
    return [MagicMock(name="entity_" + identifier, identifier=identifier) for identifier in identifiers]
