from dataclasses import is_dataclass

from link.external.entity import TableEntity


def test_if_table_entity_is_dataclass():
    assert is_dataclass(TableEntity)
