from unittest.mock import MagicMock

import pytest

from link.entities import domain


@pytest.fixture
def address():
    address = MagicMock(name="address")
    address.__repr__ = MagicMock(return_value="address", spec=domain.Address)
    return address


@pytest.fixture
def identifiers():
    return ["ID" + str(i) for i in range(10)]


@pytest.fixture
def entities(identifiers):
    return [MagicMock(name="entity_" + identifier, identifier=identifier) for identifier in identifiers]


@pytest.fixture
def gateway():
    return MagicMock(name="gateway")


@pytest.fixture
def entity_creator(entities):
    entity_creator = MagicMock(name="entity_creator")
    entity_creator.create_entities.return_value = entities.copy()
    return entity_creator


@pytest.fixture
def indexes():
    return 0, 4


@pytest.fixture
def selected_identifiers(identifiers, indexes):
    return [identifiers[i] for i in indexes]
