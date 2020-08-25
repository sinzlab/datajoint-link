from unittest.mock import MagicMock, create_autospec

import pytest

from link.entities.flag_manager import FlagManager
from link.use_cases import RepositoryLinkFactory, RepositoryLink


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(3)]


@pytest.fixture
def use_case_cls(request):
    return request.module.USE_CASE


@pytest.fixture
def repo_link_spy():
    return create_autospec(RepositoryLink, instance=True)


@pytest.fixture
def repo_link_factory_stub(repo_link_spy):
    return create_autospec(RepositoryLinkFactory, instance=True, return_value=repo_link_spy)


@pytest.fixture
def dummy_output_port():
    return MagicMock(name="dummy_output_port")


@pytest.fixture
def use_case(use_case_cls, repo_link_factory_stub, dummy_output_port):
    return use_case_cls(repo_link_factory_stub, dummy_output_port)


@pytest.fixture
def create_flag_manager_spies():
    def _create_flag_manager_spies(identifiers, flags):
        spies = {}
        for identifier, flag in zip(identifiers, flags):
            spy = create_autospec(FlagManager, instance=True)
            spy.__getitem__.return_value = flag
            spies[identifier] = spy
        return spies

    return _create_flag_manager_spies
