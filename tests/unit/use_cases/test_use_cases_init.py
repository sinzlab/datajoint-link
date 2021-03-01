from typing import Type
from unittest.mock import MagicMock, call

import pytest

from dj_link import use_cases
from dj_link.base import Base
from dj_link.entities.repository import RepositoryFactory


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
    return MagicMock(name="gateway_link_stub", spec=use_cases.AbstractGatewayLink)


@pytest.fixture
def factory(repo_factory_cls_spy, gateway_link_stub):
    class RepositoryLinkFactory(use_cases.RepositoryLinkFactory):
        repo_factory_cls = repo_factory_cls_spy

    return RepositoryLinkFactory(gateway_link_stub)


class TestRepositoryLinkFactory:
    def test_if_subclass_of_base(self):
        assert issubclass(use_cases.RepositoryLinkFactory, Base)

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

    def test_if_repo_link_is_returned(self, factory):
        assert isinstance(factory(), use_cases.RepositoryLink)

    def test_if_source_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos):
        assert factory().source is repos["source"]

    def test_if_outbound_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos):
        assert factory().outbound is repos["outbound"]

    def test_if_local_attribute_of_returned_repo_link_is_correctly_set(self, factory, repos):
        assert factory().local is repos["local"]


class TestInitializeUseCases:
    @pytest.fixture
    def dummy_output_ports(self):
        return {n: MagicMock(name="dummy_" + n + "_output_port") for n in use_cases.USE_CASES}

    @pytest.fixture(params=use_cases.USE_CASES)
    def use_case_name(self, request):
        return request.param

    @pytest.fixture
    def dummy_output_port(self, use_case_name, dummy_output_ports):
        return dummy_output_ports[use_case_name]

    @pytest.fixture
    def use_case(self, gateway_link_stub, dummy_output_ports, use_case_name):
        return use_cases.initialize_use_cases(gateway_link_stub, dummy_output_ports)[use_case_name]

    @pytest.fixture
    def use_case_cls(self, use_case_name):
        return use_cases.USE_CASES[use_case_name]

    def test_if_correct_use_case_is_returned(self, use_case, use_case_cls):
        assert isinstance(use_case, use_case_cls)

    def test_if_use_case_is_associated_with_repo_link_factory(self, use_case):
        assert isinstance(use_case.repo_link_factory, use_cases.RepositoryLinkFactory)

    def test_if_repo_link_factory_is_associated_with_gateway_link(self, use_case, gateway_link_stub):
        assert use_case.repo_link_factory.gateway_link is gateway_link_stub

    def test_if_output_port_of_use_case_is_correct(self, use_case, dummy_output_port):
        assert use_case.output_port is dummy_output_port
