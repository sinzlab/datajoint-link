from itertools import combinations
from unittest.mock import create_autospec

import pytest

from dj_link.adapters.datajoint import AbstractTableFacadeLink
from dj_link.base import Base
from dj_link.frameworks.datajoint import TableFacadeLink, initialize_frameworks
from dj_link.frameworks.datajoint.facade import TableFacade
from dj_link.frameworks.datajoint.factory import TableFactory
from dj_link.frameworks.datajoint.file import ReusableTemporaryDirectory
from dj_link.frameworks.datajoint.link import Link
from dj_link.frameworks.datajoint.mixin import LocalTableMixin


def test_if_subclass_of_abstract_table_facade_link():
    assert issubclass(TableFacadeLink, AbstractTableFacadeLink)


def test_if_subclass_of_base():
    assert issubclass(TableFacadeLink, Base)


@pytest.fixture
def facade_types():
    return "source", "outbound", "local"


@pytest.fixture
def table_facade_dummies(facade_types):
    return {facade_type: create_autospec(TableFacade) for facade_type in facade_types}


@pytest.fixture
def table_facade_link(table_facade_dummies):
    return TableFacadeLink(**table_facade_dummies)


def test_if_table_facades_are_stored_as_instance_attributes(table_facade_link, facade_types, table_facade_dummies):
    assert all(
        getattr(table_facade_link, facade_type) is table_facade_dummies[facade_type] for facade_type in facade_types
    )


class TestInitializeFrameworks:
    @pytest.fixture
    def facade_types(self):
        return "source", "outbound", "local"

    @pytest.fixture
    def facade_link(self, facade_types):
        return initialize_frameworks(facade_types)

    @pytest.fixture
    def table_facades(self, facade_link, facade_types):
        return {facade_type: getattr(facade_link, facade_type) for facade_type in facade_types}

    @pytest.fixture
    def table_factories(self, table_facades):
        return {
            facade_type: getattr(table_facade, "table_factory") for facade_type, table_facade in table_facades.items()
        }

    @pytest.fixture
    def temp_dirs(self, table_facades):
        return tuple(getattr(table_facade, "temp_dir") for table_facade in table_facades.values())

    def test_if_table_facade_link_is_returned(self, facade_link):
        assert isinstance(facade_link, TableFacadeLink)

    def test_if_attributes_of_facade_link_are_table_facades(self, facade_link, facade_types):
        assert all(isinstance(getattr(facade_link, facade_type), TableFacade) for facade_type in facade_types)

    def test_if_table_facades_are_initialized_with_table_factories(self, table_factories):
        assert all(isinstance(table_factory, TableFactory) for table_factory in table_factories.values())

    def test_if_table_facades_are_initialized_with_reusable_temporary_directory(self, temp_dirs):
        assert all(isinstance(temp_dir, ReusableTemporaryDirectory) for temp_dir in temp_dirs)

    def test_if_table_facades_are_initialized_with_same_reusable_temporary_directory(self, temp_dirs):
        assert all(temp_dir1 is temp_dir2 for temp_dir1, temp_dir2 in combinations(temp_dirs, 2))

    def test_if_prefix_of_reusable_temporary_directory_is_correct(self, temp_dirs):
        assert temp_dirs[0].prefix == "link_"

    def test_if_table_factories_are_stored_as_class_attribute_in_link_class(self, table_factories):
        assert all(
            table_factory is Link.table_cls_factories[facade_type]
            for facade_type, table_factory in table_factories.items()
        )

    def test_if_reusable_temporary_directory_of_local_table_mixin_is_configured(self, temp_dirs):
        assert LocalTableMixin.temp_dir is temp_dirs[0]

    def test_if_source_table_factory_of_local_table_mixin_is_configured(self, table_factories):
        assert LocalTableMixin.source_table_factory is table_factories["source"]
