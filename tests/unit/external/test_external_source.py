from unittest.mock import MagicMock, DEFAULT
from copy import deepcopy

import pytest


@pytest.fixture
def factory_type():
    return "source"


@pytest.fixture
def factory_args():
    return list()


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


def test_if_get_parts_is_none(factory):
    assert factory.get_parts is None


def test_if_schema_is_none(factory):
    assert factory.schema is None


def test_if_table_name_is_none(factory):
    assert factory.table_name is None


@pytest.mark.usefixtures("configure")
class TestPartsProperty:
    def test_if_get_parts_is_called_correctly(self, factory, spawned_table_cls, get_parts):
        _ = factory.parts
        get_parts.assert_called_once_with(spawned_table_cls)

    def test_if_parts_are_returned(self, factory):
        assert factory.parts == "parts"


@pytest.mark.usefixtures("configure")
class TestSpawnTableClass:
    def test_if_missing_classes_are_spawned(self, factory, schema, copy_call_args):
        new_mock = copy_call_args(schema.spawn_missing_classes)
        factory.spawn_table_cls()
        new_mock.assert_called_once_with(context=dict())

    def test_if_table_cls_is_returned(self, factory, spawned_table_cls):
        assert factory.spawn_table_cls() is spawned_table_cls


@pytest.mark.usefixtures("configure")
def test_if_source_table_is_returned_when_factory_is_called(factory, spawned_table_cls):
    assert isinstance(factory(), spawned_table_cls)


def test_repr(factory):
    assert repr(factory) == "SourceTableFactory()"
