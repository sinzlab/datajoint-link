from unittest.mock import MagicMock

import pytest

from link.entities import configuration


@pytest.fixture
def address():
    return MagicMock(name="address", spec=configuration.Address)


@pytest.fixture
def n_identifiers():
    return 10


@pytest.fixture
def n_new_identifiers():
    return 10


@pytest.fixture
def identifiers(n_identifiers):
    return ["ID" + str(i) for i in range(n_identifiers)]


@pytest.fixture
def new_identifiers(n_identifiers, n_new_identifiers):
    return ["ID" + str(n_identifiers + i) for i in range(n_new_identifiers)]


@pytest.fixture
def entity_cls():
    class Entity:
        def __init__(self, identifier):
            self.identifier = identifier

        def __repr__(self):
            return f"{self.__class__.__qualname__}({self.identifier})"

    return Entity


@pytest.fixture
def entities(identifiers, entity_cls):
    return [entity_cls(identifier) for identifier in identifiers]


@pytest.fixture
def new_entities(new_identifiers, entity_cls):
    return [entity_cls(identifier) for identifier in new_identifiers]


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
