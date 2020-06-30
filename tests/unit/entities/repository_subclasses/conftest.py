from unittest.mock import MagicMock

import pytest

from link.entities.link import Link


@pytest.fixture
def entity_cls(entity_cls):
    class FlaggedEntity(entity_cls):
        def __init__(self, identifier):
            super().__init__(identifier)
            self.deletion_requested = False
            self.deletion_approved = False

    return FlaggedEntity


@pytest.fixture
def new_selected_identifiers(new_identifiers, indexes):
    return [new_identifiers[index] for index in indexes]


@pytest.fixture
def configure_repo_cls(gateway, entity_creator):
    def _configure_repo_cls(repo_cls):
        repo_cls.__qualname__ = repo_cls.__name__
        repo_cls.gateway = gateway
        repo_cls.entity_creator = entity_creator

    return _configure_repo_cls


@pytest.fixture
def link():
    return MagicMock(name="link", spec=Link)


@pytest.fixture
def request_deletion_of_present_entities(entities, selected_identifiers, request_deletion):
    request_deletion(selected_identifiers, entities)


@pytest.fixture
def request_deletion():
    def _request_deletion(identifiers, entities):
        for entity in entities:
            if entity.identifier in identifiers:
                entity.deletion_requested = True

    return _request_deletion


@pytest.fixture
def request_deletion_of_new_entities(new_entities, new_selected_identifiers, request_deletion):
    request_deletion(new_selected_identifiers, new_entities)
