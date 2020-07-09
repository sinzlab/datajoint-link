from typing import Type

from datajoint import Lookup, Part
from datajoint.table import Table

from .source import SourceTableFactory


class OutboundTableFactory(SourceTableFactory):
    def __init__(self, table_cls: Type[Table]) -> None:
        super().__init__()
        self.table_cls = table_cls

    def __call__(self) -> Table:
        self.table_cls.__name__ = self.table_name + "Outbound"
        self.schema(self.table_cls)
        return self.table_cls()

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.table_cls})"


class OutboundTable(Lookup):
    source_table_cls = None
    definition = """
    -> self.source_table_cls
    """

    class DeletionRequested(Part):
        definition = """
        -> master
        """

    class DeletionApproved(Part):
        definition = """
        -> master
        """
