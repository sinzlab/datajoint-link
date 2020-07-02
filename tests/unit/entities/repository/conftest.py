import pytest


@pytest.fixture
def configured_repo_cls(gateway, entity_creator, repo_cls):
    repo_cls.gateway = gateway
    repo_cls.entity_creator = entity_creator
    return repo_cls


@pytest.fixture
def repo(configured_repo_cls):
    return configured_repo_cls()
