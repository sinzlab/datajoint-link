from unittest.mock import create_autospec

import pytest

from dj_link.adapters.datajoint import AbstractTableFacadeLink
from dj_link.base import Base
from dj_link.frameworks.datajoint import TableFacadeLink
from dj_link.frameworks.datajoint.facade import TableFacade


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
