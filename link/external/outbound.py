from typing import Type

from datajoint import Lookup, Part
from datajoint.table import Table
from datajoint.errors import LostConnectionError

from .source import SourceTableFactory


class OutboundTableFactory(SourceTableFactory):
    def __init__(self, table_cls: Type[Table]) -> None:
        super().__init__()
        self.table_cls = table_cls

    def __call__(self) -> Table:
        try:
            table_cls = self.spawn_table_cls()
        except KeyError:
            try:
                table_cls = self.create_table_cls()
            except LostConnectionError:
                raise RuntimeError
        return table_cls()

    def create_table_cls(self) -> Type[Table]:
        self.table_cls.__name__ = self.table_name + "Outbound"
        self.schema(self.table_cls)
        return self.table_cls

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
