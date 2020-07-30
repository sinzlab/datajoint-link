from unittest.mock import MagicMock, call
from typing import Type

import pytest

from link import use_cases
from link.use_cases.pull import Pull
from link.entities.repository import RepositoryFactory


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
def factory(repo_factory_cls_spy, gateway_link_stub):
    class RepositoryLinkFactory(use_cases.RepositoryLinkFactory):
        repo_factory_cls = repo_factory_cls_spy

    RepositoryLinkFactory.__qualname__ = RepositoryLinkFactory.__name__
    return RepositoryLinkFactory(gateway_link_stub)


class TestRepositoryLinkFactory:
    def test_if_repo_factory_cls_is_correct(self):
        assert use_cases.RepositoryLinkFactory.repo_factory_cls is RepositoryFactory

    def test_if_gateway_link_is_stored_as_instance_attribute(self, factory, gateway_link_stub):
        assert factory.gateway_link is gateway_link_stub

    def test_if_repo_factory_classes_are_correctly_initialized(
        self, factory, gateway_link_stub, repo_factory_cls_spy, kinds
    ):
        factory()
        assert repo_factory_cls_spy.call_args_list == [call(getattr(gateway_link_stub, kind)) for kind in kinds]

    def test_if_repo_factories_are_correctly_called(self, factory, repo_factory_cls_spy):
        factory()
        assert repo_factory_cls_spy.return_value.call_args_list == [call() for _ in range(3)]

    def test_if_repo_link_is_returned(
        self, factory,
    ):
        assert isinstance(factory(), use_cases.RepositoryLink)

    def test_if_source_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos):
        assert factory().source is repos["source"]

    def test_if_outbound_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos):
        assert factory().outbound is repos["outbound"]

    def test_if_local_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos):
        assert factory().local is repos["local"]

    def test_repr(self, factory):
        assert repr(factory) == "RepositoryLinkFactory(gateway_link=gateway_link_stub)"


class TestInitialize:
    @pytest.fixture
    def dummy_output_ports(self):
        return dict(pull=MagicMock(name="pull_dummy_output_port"))

    @pytest.fixture
    def returned(self, gateway_link_stub, dummy_output_ports):
        return use_cases.initialize(gateway_link_stub, dummy_output_ports)

    def test_if_dict_is_returned(self, returned):
        assert isinstance(returned, dict)

    def test_if_dict_has_correct_keys(self, returned):
        assert list(returned.keys()) == ["pull"]

    def test_if_pull_key_contains_pull_use_case(self, returned):
        assert isinstance(returned["pull"], Pull)

    def test_if_pull_use_case_is_associated_with_repo_link_factory(self, returned):
        assert isinstance(returned["pull"].repo_link_factory, use_cases.RepositoryLinkFactory)

    def test_if_repo_link_factory_is_associated_with_gateway_link(self, returned, gateway_link_stub):
        assert returned["pull"].repo_link_factory.gateway_link is gateway_link_stub

    def test_if_output_port_of_pull_use_case_is_correct(self, returned, dummy_output_ports):
        assert returned["pull"].output_port is dummy_output_ports["pull"]
