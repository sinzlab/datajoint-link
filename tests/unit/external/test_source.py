from unittest.mock import MagicMock, DEFAULT
from copy import deepcopy

import pytest

from link.external import source


@pytest.fixture
def factory():
    return source.SourceTableFactory()


def test_if_schema_is_none(factory):
    assert factory.schema is None


def test_if_table_name_is_none(factory):
    assert factory.table_name is None


@pytest.fixture
def table_name():
    return "source_table"


@pytest.fixture
def source_table():
    return MagicMock(name="source_table")


@pytest.fixture
def source_table_cls(source_table):
    return MagicMock(name="source_table_cls", return_value=source_table)


@pytest.fixture
def source_schema(table_name, source_table_cls):
    class SourceSchema:
        @staticmethod
        def spawn_missing_classes(context=None):
            context[table_name] = source_table_cls

    SourceSchema.spawn_missing_classes = MagicMock(
        name="SourceSchema.spawn_missing_classes", wraps=SourceSchema.spawn_missing_classes
    )
    return SourceSchema()


@pytest.fixture
def configure(factory, source_schema, table_name):
    factory.schema = source_schema
    factory.table_name = table_name


@pytest.fixture
def copy_call_args():
    def _copy_call_args(mock):
        new_mock = MagicMock()

        def side_effect(*args, **kwargs):
            args = deepcopy(args)
            kwargs = deepcopy(kwargs)
            new_mock(*args, **kwargs)
            return DEFAULT

        mock.side_effect = side_effect
        return new_mock

    return _copy_call_args


@pytest.mark.usefixtures("configure")
def test_if_missing_classes_are_spawned(factory, source_schema, copy_call_args):
    new_mock = copy_call_args(source_schema.spawn_missing_classes)
    factory()
    new_mock.assert_called_once_with(context=dict())


@pytest.mark.usefixtures("configure")
def test_if_source_table_cls_is_instantiated(factory, source_table_cls):
    factory()
    source_table_cls.assert_called_once_with()


@pytest.mark.usefixtures("configure")
def test_if_source_table_is_returned(factory, source_table):
    assert factory() == source_table


def test_repr(factory):
    assert repr(factory) == "SourceTableFactory()"
