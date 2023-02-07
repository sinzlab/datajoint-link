from contextlib import AbstractContextManager
from tempfile import TemporaryDirectory
from unittest.mock import create_autospec

import pytest

from dj_link.base import Base
from dj_link.frameworks.datajoint.file import ReusableTemporaryDirectory


def test_if_subclass_of_abstract_context_manager():
    assert issubclass(ReusableTemporaryDirectory, AbstractContextManager)


def test_if_subclass_of_base():
    assert issubclass(ReusableTemporaryDirectory, Base)


def test_if_temporary_directory_class_is_correct():
    assert ReusableTemporaryDirectory.temp_dir_cls is TemporaryDirectory


@pytest.fixture
def temp_dir_spy():
    spy = create_autospec(TemporaryDirectory, instance=True)
    spy.name = "temp_dir"
    return spy


@pytest.fixture
def temp_dir_cls_spy(temp_dir_spy):
    return create_autospec(TemporaryDirectory, return_value=temp_dir_spy)


@pytest.fixture
def reusable_temp_dir(temp_dir_cls_spy):
    temp_dir = ReusableTemporaryDirectory("prefix")
    temp_dir.temp_dir_cls = temp_dir_cls_spy
    return temp_dir


class TestInit:
    def test_if_prefix_is_stored_as_attribute(self, reusable_temp_dir):
        assert reusable_temp_dir.prefix == "prefix"

    def test_if_accessing_name_outside_of_context_manager_raises_attribute_error(self, reusable_temp_dir):
        with pytest.raises(AttributeError):
            _ = reusable_temp_dir.name


class TestEnter:
    def test_if_call_to_temporary_directory_class_is_correct(self, reusable_temp_dir, temp_dir_cls_spy):
        with reusable_temp_dir:
            pass
        temp_dir_cls_spy.assert_called_once_with(prefix="prefix")

    def test_if_name_is_bound_to_identifier_in_with_clause(self, reusable_temp_dir):
        with reusable_temp_dir as name:
            assert name == "temp_dir"

    def test_if_name_attribute_is_set_to_name_of_temporary_directory(self, reusable_temp_dir):
        with reusable_temp_dir as name:
            assert reusable_temp_dir.name == name


class TestExit:
    def test_if_temporary_directory_is_cleaned_up(self, reusable_temp_dir, temp_dir_spy):
        with reusable_temp_dir:
            pass
        temp_dir_spy.cleanup.assert_called_once_with()

    def test_if_name_is_deleted(self, reusable_temp_dir):
        with reusable_temp_dir:
            pass
        with pytest.raises(AttributeError):
            _ = reusable_temp_dir.name
