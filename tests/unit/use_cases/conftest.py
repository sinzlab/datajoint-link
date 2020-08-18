from unittest.mock import MagicMock, create_autospec

import pytest

from link.use_cases import RepositoryLinkFactory


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(3)]


@pytest.fixture
def repo_link_factory_stub(repo_link_spy):
    return create_autospec(RepositoryLinkFactory, instance=True, return_value=repo_link_spy)


@pytest.fixture
def dummy_output_port():
    return MagicMock(name="dummy_output_port")


@pytest.fixture
def use_case(request, repo_link_factory_stub, dummy_output_port):
    return request.module.USE_CASE(repo_link_factory_stub, dummy_output_port)
