from typing import Type

from datajoint import Part
from datajoint.table import Table

from .outbound import OutboundTableFactory
from .source import SourceTableFactory


class LocalTableFactory(OutboundTableFactory):
    replace_stores = None

    def __init__(self, table_cls: Type[Table], source_table_factory: SourceTableFactory) -> None:
        super().__init__(table_cls)
        self.source_table_factory = source_table_factory

    def spawn_table_cls(self) -> Type[Table]:
        local_table_cls = super().spawn_table_cls()
        # noinspection PyTypeChecker
        return type(self.table_name, (self.table_cls, local_table_cls), dict())

    def create_table_cls(self) -> Type[Table]:
        local_table_cls = super().create_table_cls()
        local_table_cls.definition = self.replace_stores(str(self.source_table_factory().heading))
        for name, part in self.source_table_factory().parts.items():
            part_definition = "-> master\n" + self.replace_stores(str(part.heading))
            setattr(local_table_cls, name, type(name, (Part,), dict(definition=part_definition)))
        return local_table_cls

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.table_cls}, {self.source_table_factory})"
