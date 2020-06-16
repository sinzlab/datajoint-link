from unittest.mock import MagicMock

import pytest

from link.adapters.link import LinkController


@pytest.fixture
def initialize_use_case():
    return MagicMock(name="initialize_use_case")


@pytest.fixture
def controller(initialize_use_case):
    return LinkController(initialize_use_case)


class TestInit:
    def test_if_initialize_use_case_is_stored_as_instance_attribute(self, controller, initialize_use_case):
        assert controller.initialize_use_case is initialize_use_case


class TestCall:
    def test_if_initialize_use_case_is_correctly_called(self, controller, initialize_use_case):
        controller.initialize("Table", "local_host", "local_database", "source_host", "source_database")
        initialize_use_case.assert_called_once_with(
            "Table", "local_host", "local_database", "source_host", "source_database"
        )
