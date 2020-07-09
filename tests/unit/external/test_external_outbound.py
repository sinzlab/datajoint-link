import pytest
from datajoint import Lookup, Part

from link.external.outbound import OutboundTable
from link.external.source import SourceTableFactory


@pytest.fixture
def factory_type():
    return "outbound"


@pytest.fixture
def factory_args(table_cls):
    return [table_cls]


class TestOutboundTableFactory:
    def test_if_subclass_of_source_table_factory(self, factory_cls):
        assert issubclass(factory_cls, SourceTableFactory)

    def test_if_table_cls_is_stored_as_instance_attribute(self, factory, table_cls):
        assert factory.table_cls is table_cls

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
        assert repr(factory) == "OutboundTableFactory(outbound_table_cls)"


class TestOutboundTable:
    def test_if_lookup_table(self):
        assert issubclass(OutboundTable, Lookup)

    def test_if_source_table_cls_is_none(self):
        assert OutboundTable.source_table_cls is None

    def test_if_definition_is_correct(self):
        assert OutboundTable.definition.strip() == "-> self.source_table_cls"

    def test_if_deletion_requested_is_part_table(self):
        assert issubclass(OutboundTable.DeletionRequested, Part)

    def test_if_definition_of_deletion_requested_part_table_is_correct(self):
        assert OutboundTable.DeletionRequested.definition.strip() == "-> master"

    def test_if_deletion_approved_is_part_table(self):
        assert issubclass(OutboundTable.DeletionApproved, Part)

    def test_if_definition_of_deletion_approved_part_table_is_correct(self):
        assert OutboundTable.DeletionApproved.definition.strip() == "-> master"
