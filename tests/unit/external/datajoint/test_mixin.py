from unittest.mock import MagicMock, create_autospec

import pytest
from datajoint import AndList

from link.external.datajoint.mixin import LocalTableMixin
from link.external.datajoint.factory import TableFactory


@pytest.fixture
def fake_controller():
    class FakeController:
        is_in_with_clause = False

        # noinspection PyUnusedLocal
        def pull(self, *restrictions):
            if not self.is_in_with_clause:
                raise RuntimeError("Pull method must be called inside with clause")

    fake_controller = FakeController()
    fake_controller.pull = MagicMock(name="fake_controller.pull", wraps=fake_controller.pull)
    fake_controller.delete = MagicMock(name="fake_controller.delete")
    return fake_controller


@pytest.fixture
def fake_temp_dir(fake_controller):
    class FakeTemporaryDirectory:
        controller = fake_controller

        def __enter__(self):
            self.controller.is_in_with_clause = True

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.controller.is_in_with_clause = False

    return FakeTemporaryDirectory()


@pytest.fixture
def source_table_factory_spy():
    return create_autospec(TableFactory, instance=True)()


@pytest.fixture(autouse=True)
def configure_mixin(fake_controller, fake_temp_dir, source_table_factory_spy):
    LocalTableMixin._controller = fake_controller
    LocalTableMixin._temp_dir = fake_temp_dir
    LocalTableMixin._source_table_factory = source_table_factory_spy


class TestPull:
    def test_if_call_to_controller_is_correct(self, fake_controller):
        LocalTableMixin().pull("restriction1", "restriction2")
        fake_controller.pull.assert_called_once_with(("restriction1", "restriction2"))

    def test_if_call_to_controller_is_correct_if_no_restrictions_are_passed(self, fake_controller):
        LocalTableMixin().pull()
        fake_controller.pull.assert_called_once_with(AndList())


class TestDelete:
    def test_if_call_to_controller_is_correct(self, fake_controller):
        LocalTableMixin.restriction = "restriction"
        LocalTableMixin().delete()
        fake_controller.delete.assert_called_once_with("restriction")


class TestSourceProperty:
    def test_if_call_to_source_table_factory_is_correct(self, source_table_factory_spy):
        _ = LocalTableMixin().source
        source_table_factory_spy.assert_called_once_with()

    def test_if_source_table_class_is_returned(self, source_table_factory_spy):
        assert LocalTableMixin().source is source_table_factory_spy.return_value
