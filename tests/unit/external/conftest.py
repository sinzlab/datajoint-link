from unittest.mock import MagicMock

import pytest

from link.external import source, outbound, local


MODULES = dict(source=source, outbound=outbound, local=local)


@pytest.fixture
def factory_cls(factory_type):
    return getattr(MODULES[factory_type], factory_type.title() + "TableFactory")


@pytest.fixture
def factory(factory_cls, factory_args):
    return factory_cls(*factory_args)


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
def table_name():
    return "source_table"


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
