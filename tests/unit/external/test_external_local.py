from unittest.mock import MagicMock

import pytest

from link.external.outbound import OutboundTableFactory


@pytest.fixture
def factory_type():
    return "local"


@pytest.fixture
def factory_args(source_table_factory, table_cls):
    return [table_cls, source_table_factory]


@pytest.fixture
def source_table_factory():
    name = "source_table_factory"
    source_table_factory = MagicMock(name=name)
    source_table_factory.__repr__ = MagicMock(name=name + ".__repr__", return_value=name)
    return source_table_factory


def test_if_subclass_of_outbound_table_factory(factory_cls):
    assert issubclass(factory_cls, OutboundTableFactory)


def test_if_source_table_factory_is_stored_as_instance_attribute(factory, source_table_factory):
    assert factory.source_table_factory is source_table_factory
