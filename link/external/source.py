from typing import Type
from inspect import isclass

from datajoint import Part
from datajoint.table import Table


class SourceTableFactory:
    def __init__(self) -> None:
        self.schema = None
        self.table_name = None
        self._ignored_parts = []

    def __call__(self) -> Table:
        source_table_cls = self.spawn_table_cls()
        return source_table_cls()

    def spawn_table_cls(self) -> Type[Table]:
        table_classes = dict()
        self.schema.spawn_missing_classes(context=table_classes)
        table_cls = table_classes[self.table_name]
        table_cls.parts = self._get_part_tables(table_cls)
        return table_cls

    def _get_part_tables(self, table_cls):
        parts = dict()
        for name in dir(table_cls):
            if name[0].isupper() and name not in self._ignored_parts:
                attr = getattr(table_cls, name)
                if isclass(attr) and issubclass(attr, Part):
                    parts[name] = attr
        return parts

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"
