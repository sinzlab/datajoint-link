from unittest.mock import MagicMock

import pytest

from link.adapters.datajoint import local_table


@pytest.fixture
def restriction():
    return "restriction"


def test_if_pull_use_case_is_none():
    assert local_table.LocalTableController.pull_use_case is None


def test_if_source_gateway_is_none():
    assert local_table.LocalTableController.source_gateway is None


@pytest.fixture
def pull_use_case():
    return MagicMock(name="pull_use_case")


@pytest.fixture
def source_gateway(identifiers):
    source_gateway = MagicMock(name="source_gateway")
    source_gateway.get_identifiers_in_restriction.return_value = identifiers
    return source_gateway


@pytest.fixture
def controller_cls(pull_use_case, source_gateway):
    class LocalTableController(local_table.LocalTableController):
        pass

    LocalTableController.__qualname__ = LocalTableController.__name__
    LocalTableController.pull_use_case = pull_use_case
    LocalTableController.source_gateway = source_gateway
    return LocalTableController


@pytest.fixture
def controller(controller_cls):
    return controller_cls()


def test_if_restriction_is_converted_into_identifiers(source_gateway, controller, restriction):
    controller.pull(restriction)
    source_gateway.get_identifiers_in_restriction.assert_called_once_with(restriction)


def test_if_pull_use_case_is_called_with_identifiers(identifiers, pull_use_case, controller, restriction):
    controller.pull(restriction)
    pull_use_case.assert_called_once_with(identifiers)


def test_repr(controller):
    assert repr(controller) == "LocalTableController()"
