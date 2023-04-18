import logging
from unittest.mock import MagicMock, create_autospec

import pytest

from dj_link.entities.flag_manager import FlagManager
from dj_link.use_cases import REQUEST_MODELS, RESPONSE_MODELS, USE_CASES, RepositoryLink, RepositoryLinkFactory


@pytest.fixture
def identifiers(create_identifiers):
    return create_identifiers(3)


@pytest.fixture
def request_model_stub(request, identifiers):
    stub = create_autospec(REQUEST_MODELS[request.module.USE_CASE_NAME], instance=True)
    stub.identifiers = identifiers
    return stub


@pytest.fixture
def response_model_cls_spy(request):
    return create_autospec(RESPONSE_MODELS[request.module.USE_CASE_NAME])


@pytest.fixture
def use_case_cls(request, response_model_cls_spy):
    use_case_cls = type(
        USE_CASES[request.module.USE_CASE_NAME].__name__, (USE_CASES[request.module.USE_CASE_NAME],), dict()
    )
    use_case_cls.response_model_cls = response_model_cls_spy
    return use_case_cls


@pytest.fixture
def repo_link_spy():
    spy = create_autospec(RepositoryLink, instance=True)
    spy.mock_add_spec(["source", "outbound", "local"])
    return spy


@pytest.fixture
def repo_link_factory_stub(repo_link_spy):
    return create_autospec(RepositoryLinkFactory, instance=True, return_value=repo_link_spy)


@pytest.fixture
def output_port_spy():
    return MagicMock(name="output_port_spy")


@pytest.fixture
def use_case(use_case_cls, fake_gateway_link, repo_link_factory_stub, output_port_spy):
    return use_case_cls(fake_gateway_link, repo_link_factory_stub, output_port_spy)


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


@pytest.fixture
def is_correct_log(caplog):
    def _is_correct_log(logger, func, messages, log_level=logging.INFO):
        with caplog.at_level(log_level, logger=logger.name):
            func()
        logged_messages = {r.message for r in caplog.records}
        return logged_messages == set(messages)

    return _is_correct_log
