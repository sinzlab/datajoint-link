from unittest.mock import MagicMock

import pytest

from link.use_cases import pull
from link.use_cases.base import UseCase
from link.entities.local import LocalRepository
from link.entities.outbound import OutboundRepository
from link.entities.repository import SourceRepository


def test_if_subclass_of_use_case():
    assert issubclass(pull.Pull, UseCase)


def test_local_repository_class_attribute():
    assert pull.Pull.local_repo_cls is LocalRepository


def test_outbound_repository_class_attribute():
    assert pull.Pull.outbound_repo_cls is OutboundRepository


def test_source_repository_class_attribute():
    assert pull.Pull.source_repo_cls is SourceRepository


@pytest.fixture
def local_repo_cls():
    return MagicMock(name="local_repo", spec=LocalRepository)


@pytest.fixture
def outbound_repo_cls():
    return MagicMock(name="outbound_repo", spec=OutboundRepository)


@pytest.fixture
def source_repo_cls(entities):
    source_repo_cls = MagicMock(name="source_repo", spec=SourceRepository)
    source_repo_cls.return_value.fetch.return_value = entities
    return source_repo_cls


@pytest.fixture
def use_case(local_repo_cls, outbound_repo_cls, source_repo_cls):
    class Pull(pull.Pull):
        pass

    Pull.local_repo_cls = local_repo_cls
    Pull.outbound_repo_cls = outbound_repo_cls
    Pull.source_repo_cls = source_repo_cls
    return Pull


@pytest.fixture
def output_port():
    return MagicMock(name="output_port")


@pytest.fixture
def execute(identifiers, use_case, output_port):
    use_case(output_port)(identifiers)


@pytest.mark.usefixtures("execute")
def test_if_entities_are_fetched_from_source_repository(identifiers, source_repo_cls):
    source_repo_cls.return_value.fetch.assert_called_once_with(identifiers)


@pytest.mark.usefixtures("execute")
def test_if_entities_are_inserted_into_outbound_repository(entities, outbound_repo_cls):
    outbound_repo_cls.return_value.insert.assert_called_once_with(entities)


@pytest.mark.usefixtures("execute")
def test_if_entities_are_inserted_into_local_repository(entities, local_repo_cls):
    local_repo_cls.return_value.insert.assert_called_once_with(entities)
