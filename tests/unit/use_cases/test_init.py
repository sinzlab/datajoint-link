from unittest.mock import MagicMock, call
from typing import Type

import pytest

from link.entities.repository import RepositoryFactory
from link import use_cases


@pytest.fixture
def kinds():
    return "source", "outbound", "local"


@pytest.fixture
def repos(kinds):
    return {kind: MagicMock(name=kind + "_repo") for kind in kinds}


@pytest.fixture
def repo_factory_cls_spy(repos):
    repo_factory_cls_spy = MagicMock(name="repo_factory_cls_spy", spec=Type[RepositoryFactory])
    repo_factory_cls_spy.return_value.side_effect = repos.values()
    return repo_factory_cls_spy


@pytest.fixture
def gateway_link_stub():
    name = "gateway_link_stub"
    gateway_link_stub = MagicMock(name=name, spec=use_cases.AbstractGatewayLink)
    gateway_link_stub.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return gateway_link_stub


@pytest.fixture
def storage():
    return dict()


@pytest.fixture
def factory(repo_factory_cls_spy, gateway_link_stub):
    class LinkFactory(use_cases.RepositoryLinkFactory):
        repo_factory_cls = repo_factory_cls_spy

    LinkFactory.__qualname__ = LinkFactory.__name__
    return LinkFactory(gateway_link_stub)


class TestRepositoryLinkFactory:
    def test_if_repo_factory_cls_is_correct(self):
        assert use_cases.RepositoryLinkFactory.repo_factory_cls is RepositoryFactory

    def test_if_gateway_link_is_stored_as_instance_attribute(self, factory, gateway_link_stub):
        assert factory.gateway_link is gateway_link_stub

    def test_if_repo_factory_classes_are_correctly_initialized(
        self, factory, gateway_link_stub, repo_factory_cls_spy, storage, kinds
    ):
        factory(storage)
        assert repo_factory_cls_spy.call_args_list == [
            call(getattr(gateway_link_stub, kind), storage) for kind in kinds
        ]

    def test_if_repo_factories_are_correctly_called(self, factory, repo_factory_cls_spy, storage):
        factory(storage)
        assert repo_factory_cls_spy.return_value.call_args_list == [call() for _ in range(3)]

    def test_if_repo_link_is_returned(self, factory, storage):
        assert isinstance(factory(storage), use_cases.RepositoryLink)

    def test_if_source_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos, storage):
        assert factory(storage).source is repos["source"]

    def test_if_outbound_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos, storage):
        assert factory(storage).outbound is repos["outbound"]

    def test_if_local_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos, storage):
        assert factory(storage).local is repos["local"]

    def test_repr(self, factory):
        assert repr(factory) == "LinkFactory(gateway_link=gateway_link_stub)"
