from unittest.mock import MagicMock

import pytest


@pytest.fixture
def n_new_identifiers():
    return 10


@pytest.fixture
def new_identifiers(n_identifiers, n_new_identifiers):
    return ["ID" + str(n_identifiers + i) for i in range(n_new_identifiers)]


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


@pytest.fixture
def configured_repo_cls(gateway, entity_creator, repo_cls, storage):
    repo_cls.gateway = gateway
    repo_cls.entity_creator = entity_creator
    repo_cls.storage = storage
    return repo_cls
