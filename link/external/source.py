from datajoint.table import Table


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
