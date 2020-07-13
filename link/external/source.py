from typing import Type

from datajoint.table import Table


class SourceTableFactory:
    get_parts = None

    def __init__(self) -> None:
        self.schema = None
        self.table_name = None
        self._ignored_parts = []

    def __call__(self) -> Table:
        source_table_cls = self.spawn_table_cls()
        return source_table_cls()

    @property
    def parts(self):
        return self.get_parts(self.spawn_table_cls())

    def spawn_table_cls(self) -> Type[Table]:
        table_classes = dict()
        self.schema.spawn_missing_classes(context=table_classes)
        table_cls = table_classes[self.table_name]
        return table_cls

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"
