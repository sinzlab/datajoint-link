from unittest.mock import MagicMock, create_autospec

import pytest
from datajoint import AndList

from dj_link.frameworks.datajoint.factory import TableFactory
from dj_link.frameworks.datajoint.mixin import LocalTableMixin
from dj_link.use_cases import USE_CASES


@pytest.fixture()
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
    fake_controller.refresh = MagicMock(name="fake_controller.refresh")
    return fake_controller


@pytest.fixture()
def fake_temp_dir(fake_controller):
    class FakeTemporaryDirectory:
        controller = fake_controller

        def __enter__(self):
            self.controller.is_in_with_clause = True

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.controller.is_in_with_clause = False

    return FakeTemporaryDirectory()


@pytest.fixture()
def printer_spy():
    return MagicMock(name="printer_spy")


@pytest.fixture()
def source_table_factory_spy():
    return create_autospec(TableFactory, instance=True)()


@pytest.fixture(autouse=True)
def configure_mixin(fake_controller, fake_temp_dir, source_table_factory_spy, printer_spy):
    LocalTableMixin.controller = fake_controller
    LocalTableMixin.temp_dir = fake_temp_dir
    LocalTableMixin.source_table_factory = source_table_factory_spy
    LocalTableMixin.printer = printer_spy


class TestPull:
    def test_if_call_to_controller_is_correct(self, fake_controller):
        LocalTableMixin().pull("restriction1", "restriction2")
        fake_controller.pull.assert_called_once_with(("restriction1", "restriction2"))

    def test_if_call_to_controller_is_correct_if_no_restrictions_are_passed(self, fake_controller):
        LocalTableMixin().pull()
        fake_controller.pull.assert_called_once_with(AndList())


def test_if_call_to_controller_is_correct(fake_controller):
    LocalTableMixin.restriction = "restriction"
    LocalTableMixin().delete()
    fake_controller.delete.assert_called_once_with("restriction")


def test_if_call_to_controller_is_correct_when_refreshing(fake_controller):
    LocalTableMixin().refresh()
    fake_controller.refresh.assert_called_once_with()


@pytest.mark.parametrize("method_name", USE_CASES)
def test_if_call_to_printer_is_correct(printer_spy, method_name):
    getattr(LocalTableMixin(), method_name)()
    printer_spy.assert_called_once_with()


@pytest.mark.parametrize("method_name", USE_CASES)
def test_if_printer_is_called_after_use_case_is_executed(fake_controller, printer_spy, method_name):
    setattr(fake_controller, method_name, MagicMock(side_effect=RuntimeError))
    try:
        getattr(LocalTableMixin(), method_name)()
    except RuntimeError:
        pass
    printer_spy.assert_not_called()


class TestSourceProperty:
    def test_if_call_to_source_table_factory_is_correct(self, source_table_factory_spy):
        _ = LocalTableMixin().source
        source_table_factory_spy.assert_called_once_with()

    def test_if_source_table_class_is_returned(self, source_table_factory_spy):
        assert LocalTableMixin().source is source_table_factory_spy.return_value
