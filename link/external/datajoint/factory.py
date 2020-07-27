from __future__ import annotations
from typing import Optional, List, Dict, Type, Any
from dataclasses import dataclass

from datajoint import Schema, Lookup, Part


@dataclass(frozen=True)
class SpawnTableConfig:
    schema: Schema
    table_name: str
    table_cls_attrs: Dict[str, Any]
    flag_table_names: List[str]


@dataclass(frozen=True)
class CreateTableConfig:
    table_definition: str
    part_table_definitions: Dict[str, str]


class TableFactory:
    def __init__(self) -> None:
        self.spawn_table_config: Optional[SpawnTableConfig] = None
        self.create_table_config: Optional[CreateTableConfig] = None

    def __call__(self) -> Lookup:
        if self.spawn_table_config is None:
            raise RuntimeError
        try:
            table_cls = self._spawn_table_cls()
        except KeyError:
            if self.create_table_config is None:
                raise RuntimeError
            table_cls = self._create_table_cls()
        return table_cls()

    @property
    def part_tables(self) -> Dict[Part]:
        # 1. Access table
        # 2. Get part tables from table
        # 3. Filter out flag tables
        # 4. Return remaining part tables
        pass

    @property
    def flag_tables(self) -> Dict[Part]:
        # 1. Access table
        # 2. Get part tables from table
        # 3. Filter out non-flag tables
        # 4. Return flag tables
        pass

    def _spawn_table_cls(self) -> Type[Lookup]:
        spawned_table_classes = dict()
        self.spawn_table_config.schema.spawn_missing_classes(context=spawned_table_classes)
        table_cls = spawned_table_classes[self.spawn_table_config.table_name]
        self._set_attrs_on_table_cls(table_cls)
        return table_cls

    def _create_table_cls(self) -> Type[Lookup]:
        table_cls = self._create_master_table_cls()
        table_cls_attrs = self._create_table_cls_attrs()
        self._set_attrs_on_table_cls(table_cls, table_cls_attrs)
        return self.spawn_table_config.schema(table_cls)

    def _create_master_table_cls(self):
        return type(
            self.spawn_table_config.table_name, (Lookup,), dict(definition=self.create_table_config.table_definition),
        )

    def _create_table_cls_attrs(self):
        table_cls_attrs = dict()
        self._create_flag_table_classes(table_cls_attrs)
        self._create_non_flag_part_table_classes(table_cls_attrs)
        return table_cls_attrs

    def _create_flag_table_classes(self, table_cls_attrs):
        table_cls_attrs.update(
            self._create_part_table_classes({name: "-> master" for name in self.spawn_table_config.flag_table_names})
        )

    def _create_non_flag_part_table_classes(self, table_cls_attrs):
        table_cls_attrs.update(self._create_part_table_classes(self.create_table_config.part_table_definitions))

    def _create_part_table_classes(self, definitions):
        part_tables = dict()
        for name, definition in definitions.items():
            part_tables[name] = self._create_part_table_cls(name, definition)
        return part_tables

    @staticmethod
    def _create_part_table_cls(name, definition):
        return type(name, (Part,), dict(definition=definition))

    def _set_attrs_on_table_cls(self, table_cls, additional_table_cls_attrs=None):
        table_cls_attrs = self.spawn_table_config.table_cls_attrs
        if additional_table_cls_attrs:
            table_cls_attrs = {**table_cls_attrs, **additional_table_cls_attrs}
        for attr_name, attr_value in table_cls_attrs.items():
            setattr(table_cls, attr_name, attr_value)
