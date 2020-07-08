from unittest.mock import MagicMock

import pytest

from link.adapters import local_table


@pytest.fixture
def identifiers():
    return [
        "62aad6b1b90f0613ac14b3ed0f5ecbf1c3cca448",
        "2d78c5aafa6200eb909bfc7b4b5b8f07284ad734",
        "e359f33515accad6b2e967135ee713cd17a200c9",
        "f62ac0bf9e4f661e617b935c76076bdfb5845cf3",
        "9f1d3a454a02283d83d2da2b02ce8950fb683d14",
        "f355683595377472c79473009e2cef9259254359",
    ]


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


@pytest.fixture
def restriction():
    return "restriction"


def test_if_restriction_is_converted_into_identifiers(source_gateway, controller, restriction):
    controller.pull(restriction)
    source_gateway.get_identifiers_in_restriction.assert_called_once_with(restriction)


def test_if_pull_use_case_is_called_with_identifiers(identifiers, pull_use_case, controller, restriction):
    controller.pull(restriction)
    pull_use_case.assert_called_once_with(identifiers)


def test_repr(controller):
    assert repr(controller) == "LocalTableController()"
