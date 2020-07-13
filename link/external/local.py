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
        part_definitions = self._create_part_definitions()
        parts = self._create_part_tables(part_definitions)
        self._assign_part_tables(local_table_cls, parts)
        local_table_cls.parts = parts
        return local_table_cls

    def _create_part_definitions(self):
        part_definitions = []
        for part in self.source_table_factory.parts.values():
            part_definitions.append("-> master\n" + self.replace_stores(str(part.heading)))
        return part_definitions

    def _create_part_tables(self, part_definitions):
        parts = dict()
        for (name, part), definition in zip(self.source_table_factory.parts.items(), part_definitions):
            parts[name] = type(name, (Part,), dict(definition=definition))
        return parts

    @staticmethod
    def _assign_part_tables(local_table_cls, parts):
        for name, part in parts.items():
            setattr(local_table_cls, name, part)

    def __repr__(self) -> str:
        return f"{self.__class__.__qualname__}({self.table_cls}, {self.source_table_factory})"
