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
def make_table_cls(factory_type):
    def _make_table_cls(kind):
        return type(factory_type.title() + kind.title() + "Table", tuple(), dict())

    return _make_table_cls


@pytest.fixture
def spawned_table_cls(make_table_cls):
    return make_table_cls("spawned")


@pytest.fixture
def created_table_cls(make_table_cls):
    return make_table_cls("created")


@pytest.fixture
def table_name():
    return "SourceTable"


@pytest.fixture
def schema(factory_type, table_name, spawned_table_cls, created_table_cls):
    class Schema:
        @staticmethod
        def spawn_missing_classes(context=None):
            context[table_name] = spawned_table_cls

        def __call__(self, _):
            return created_table_cls

    Schema.__name__ = factory_type.title() + Schema.__name__
    Schema.spawn_missing_classes = MagicMock(
        name=factory_type.title() + "Schema.spawn_missing_classes", wraps=Schema.spawn_missing_classes
    )
    return MagicMock(name=Schema.__name__, wraps=Schema())


@pytest.fixture
def configure(factory, schema, table_name):
    factory.schema = schema
    factory.table_name = table_name
