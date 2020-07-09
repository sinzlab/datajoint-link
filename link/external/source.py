from datajoint import Lookup, Part
from datajoint.table import Table
from datajoint.errors import LostConnectionError


class SourceTableFactory:
    def __init__(self) -> None:
        self.schema = None
        self.table_name = None

    def __call__(self) -> Table:
        source_tables = dict()
        self.schema.spawn_missing_classes(context=source_tables)
        source_table_cls = source_tables[self.table_name]
        return source_table_cls()

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "()"


class OutboundTableFactory(SourceTableFactory):
    def __init__(self, source_table_cls, table_cls):
        super().__init__()
        self.source_table_cls = source_table_cls
        self.table_cls = table_cls

    def __call__(self):
        self.table_cls.source_table_cls = self.source_table_cls
        self.table_cls.__name__ = self.table_name + "Outbound"
        self.schema(self.table_cls)
        return self.table_cls()

    def __repr__(self):
        return f"{self.__class__.__qualname__}({self.source_table_cls}, {self.table_cls})"


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


class LocalTableFactory(OutboundTableFactory):
    def __call__(self):
        try:
            return self.spawn_table()
        except KeyError:
            try:
                return self.create_table()
            except LostConnectionError:
                raise RuntimeError

    def spawn_table(self):
        pass

    def create_table(self):
        pass
