from unittest.mock import MagicMock

import pytest
from datajoint import AndList

from link.external.datajoint.mixin import LocalTableMixin


@pytest.fixture
def fake_controller():
    class FakeController:
        is_in_with_clause = False

        # noinspection PyUnusedLocal
        def pull(self, *restrictions):
            if not self.is_in_with_clause:
                raise RuntimeError("Pull method must be called inside with clause")

    fake_controller = FakeController()
    fake_controller.pull = MagicMock(wraps=fake_controller.pull)
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


@pytest.fixture(autouse=True)
def configure_mixin(fake_controller, fake_temp_dir):
    LocalTableMixin._controller = fake_controller
    LocalTableMixin._temp_dir = fake_temp_dir


class TestPull:
    def test_if_call_to_controller_is_correct(self, fake_controller):
        LocalTableMixin().pull("restriction1", "restriction2")
        fake_controller.pull.assert_called_once_with(("restriction1", "restriction2"))

    def test_if_call_to_controller_is_correct_if_no_restrictions_are_passed(self, fake_controller):
        LocalTableMixin().pull()
        fake_controller.pull.assert_called_once_with(AndList())
