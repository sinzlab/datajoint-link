from typing import Type

from datajoint.table import Table

from .outbound import OutboundTableFactory
from .source import SourceTableFactory


class LocalTableFactory(OutboundTableFactory):
    def __init__(self, table_cls: Type[Table], source_table_factory: SourceTableFactory) -> None:
        super().__init__(table_cls)
        self.source_table_factory = source_table_factory

    def spawn_table_cls(self) -> Type[Table]:
        local_table_cls = super().spawn_table_cls()
        # noinspection PyTypeChecker
        return type(self.source_table_factory().__name__, (self.table_cls, local_table_cls), dict())

    def create_table_cls(self) -> Type[Table]:
        pass

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.table_cls}, {self.source_table_factory})"
