from typing import Type

from datajoint.table import Table
from datajoint.errors import LostConnectionError

from .outbound import OutboundTableFactory
from .source import SourceTableFactory


class LocalTableFactory(OutboundTableFactory):
    def __init__(self, table_cls: Type[Table], source_table_factory: SourceTableFactory) -> None:
        super().__init__(table_cls)
        self.source_table_factory = source_table_factory

    def __call__(self) -> Table:
        try:
            table_cls = self.spawn_table_cls()
        except KeyError:
            try:
                table_cls = self.create_table_cls()
            except LostConnectionError:
                raise RuntimeError
        return table_cls()

    def spawn_table_cls(self) -> Type[Table]:
        pass

    def create_table_cls(self) -> Type[Table]:
        pass
