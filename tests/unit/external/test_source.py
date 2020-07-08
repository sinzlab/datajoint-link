from unittest.mock import MagicMock, DEFAULT
from copy import deepcopy

import pytest

from link.external import source


@pytest.fixture
def factory_args():
    return list()


@pytest.fixture
def factory(factory_cls, factory_args):
    return factory_cls(*factory_args)


@pytest.fixture
def table_name():
    return "source_table"


@pytest.fixture
def table(factory_type):
    return MagicMock(name=factory_type + "_table")


@pytest.fixture
def table_cls(factory_type, table):
    name = factory_type + "_table_cls"
    table_cls = MagicMock(name=name, return_value=table)
    table_cls.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return table_cls


@pytest.fixture
def schema(factory_type, table_name, table_cls):
    class Schema:
        @staticmethod
        def spawn_missing_classes(context=None):
            context[table_name] = table_cls

        def __call__(self, _):
            return table_cls

    Schema.__name__ = factory_type.title() + Schema.__name__
    Schema.spawn_missing_classes = MagicMock(
        name=factory_type.title() + "Schema.spawn_missing_classes", wraps=Schema.spawn_missing_classes
    )
    return MagicMock(name=Schema.__name__, wraps=Schema())


@pytest.fixture
def configure(factory, schema, table_name):
    factory.schema = schema
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


class TestSourceTableFactory:
    @pytest.fixture
    def factory_type(self):
        return "source"

    @pytest.fixture
    def factory_cls(self):
        return source.SourceTableFactory

    def test_if_schema_is_none(self, factory):
        assert factory.schema is None

    def test_if_table_name_is_none(self, factory):
        assert factory.table_name is None

    @pytest.mark.usefixtures("configure")
    def test_if_missing_classes_are_spawned(self, factory, schema, copy_call_args):
        new_mock = copy_call_args(schema.spawn_missing_classes)
        factory()
        new_mock.assert_called_once_with(context=dict())

    @pytest.mark.usefixtures("configure")
    def test_if_source_table_cls_is_instantiated(self, factory, table_cls):
        factory()
        table_cls.assert_called_once_with()

    @pytest.mark.usefixtures("configure")
    def test_if_source_table_is_returned(self, factory, table):
        assert factory() == table

    def test_repr(self, factory):
        assert repr(factory) == "SourceTableFactory()"


class TestOutboundTableFactory:
    @pytest.fixture
    def factory_type(self):
        return "outbound"

    @pytest.fixture
    def factory_cls(self):
        return source.OutboundTableFactory

    @pytest.fixture
    def source_table_factory(self):
        name = "source_table_factory"
        source_table_factory = MagicMock(name=name)
        source_table_factory.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
        return source_table_factory

    @pytest.fixture
    def factory_args(self, source_table_factory, table_cls):
        return [source_table_factory, table_cls]

    def test_if_subclass_of_source_table_factory(self, factory_cls):
        assert issubclass(factory_cls, source.SourceTableFactory)

    def test_if_table_cls_is_stored_as_instance_attribute(self, factory, table_cls):
        assert factory.table_cls is table_cls

    def test_if_source_table_is_stored_as_instance_attribute(self, factory, source_table_factory):
        assert factory.source_table_cls is source_table_factory

    @pytest.mark.usefixtures("configure")
    def test_if_source_table_cls_attribute_of_outbound_table_cls_is_correctly_set(
        self, factory, source_table_factory, table_cls
    ):
        factory()
        assert table_cls.source_table_cls is source_table_factory

    @pytest.mark.usefixtures("configure")
    def test_if_name_attribute_of_outbound_table_cls_is_correctly_set(self, factory, table_name, table_cls):
        factory()
        assert table_cls.__name__ == table_name + "Outbound"

    @pytest.mark.usefixtures("configure")
    def test_if_outbound_schema_is_applied_to_outbound_table_class(self, factory, schema, table_cls):
        factory()
        print(schema)
        schema.assert_called_once_with(table_cls)

    @pytest.mark.usefixtures("configure")
    def test_if_outbound_table_is_returned(self, factory, table):
        assert factory() is table

    def test_repr(self, factory):
        assert repr(factory) == "OutboundTableFactory(source_table_factory, outbound_table_cls)"
