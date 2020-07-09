from typing import Type

from datajoint.table import Table

from .outbound import OutboundTableFactory
from .source import SourceTableFactory


class LocalTableFactory(OutboundTableFactory):
    def __init__(self, table_cls: Type[Table], source_table_factory: SourceTableFactory) -> None:
        super().__init__(table_cls)
        self.source_table_factory = source_table_factory

    def spawn_table_cls(self) -> Type[Table]:
        pass

    def create_table_cls(self) -> Type[Table]:
        pass
