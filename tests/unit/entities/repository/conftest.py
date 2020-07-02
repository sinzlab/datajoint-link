from unittest.mock import MagicMock

import pytest


@pytest.fixture
def data(identifiers):
    return {identifier: "data" + str(i) for i, identifier in enumerate(identifiers)}


@pytest.fixture
def gateway(gateway, data):
    gateway.fetch.return_value = data
    return gateway


@pytest.fixture
def storage():
    storage = MagicMock(name="storage")
    storage.__contains__ = MagicMock(name="storage.__contains__", return_value=False)
    return storage


@pytest.fixture
def configured_repo_cls(gateway, entity_creator, repo_cls, storage):
    repo_cls.gateway = gateway
    repo_cls.entity_creator = entity_creator
    repo_cls.storage = storage
    return repo_cls


@pytest.fixture
def repo(configured_repo_cls):
    return configured_repo_cls()
