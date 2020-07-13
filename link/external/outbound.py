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
                table_cls = self.schema(self.create_table_cls())
            except LostConnectionError:
                raise RuntimeError
        return table_cls()

    def create_table_cls(self) -> Type:
        return type(self.table_name, (self.table_cls, Lookup), dict())

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.table_cls})"


class DeletionRequested(Part):
    definition = """
    -> master
    """


class OutboundTable:
    source_table_cls = None
    DeletionRequested = DeletionRequested
    definition = """
    -> self.source_table_cls
    """

    class DeletionApproved(Part):
        definition = """
        -> master
        """
