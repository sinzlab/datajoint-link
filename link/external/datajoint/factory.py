from __future__ import annotations
from typing import Optional, List, Dict, Type, Any, Union, Tuple
from dataclasses import dataclass, field

from datajoint import Schema, Lookup, Part, Table

from .dj_helpers import get_part_table_classes
from ...base import Base


@dataclass
class TableFactoryConfig:
    schema: Schema
    table_name: str
    table_bases: Tuple[Type] = field(default_factory=tuple)
    table_cls_attrs: Dict[str, Any] = field(default_factory=dict)
    flag_table_names: List[str] = field(default_factory=list)
    table_definition: Optional[str] = None
    part_table_definitions: Dict[str, str] = field(default_factory=dict)


class TableFactory(Base):
    def __init__(self) -> None:
        self.config: Optional[TableFactoryConfig] = None

    def __call__(self) -> Union[Type[Lookup], Type[Table]]:
        if self.config is None:
            raise RuntimeError
        try:
            table_cls = self._spawn_table_cls()
        except KeyError:
            if self.config.table_definition is None:
                raise RuntimeError
            table_cls = self._create_table_cls()
        return table_cls

    @property
    def part_tables(self) -> Dict[str, Type[Part]]:
        return get_part_table_classes(self(), ignored_parts=self.config.flag_table_names)

    @property
    def flag_tables(self) -> Dict[str, Type[Part]]:
        return {name: getattr(self(), name) for name in self.config.flag_table_names}

    def _spawn_table_cls(self) -> Type:
        spawned_table_classes = dict()
        self.config.schema.spawn_missing_classes(context=spawned_table_classes)
        table_cls = spawned_table_classes[self.config.table_name]
        return self._extend_table_cls(table_cls)

    def _extend_table_cls(
        self, table_cls: Type[Table], part_table_classes: Optional[Dict[str, Type[Part]]] = None
    ) -> Type:
        if part_table_classes is None:
            part_table_classes = dict()
        if self.config.table_definition:
            table_cls_attrs = dict(self.config.table_cls_attrs, definition=self.config.table_definition)
        else:
            table_cls_attrs = self.config.table_cls_attrs
        return type(
            self.config.table_name, self.config.table_bases + (table_cls,), {**table_cls_attrs, **part_table_classes},
        )

    def _create_table_cls(self) -> Type[Lookup]:
        part_table_classes = dict()
        self._create_flag_part_table_classes(part_table_classes)
        self._create_non_flag_part_table_classes(part_table_classes)
        extended_table_cls = self._extend_table_cls(Lookup, part_table_classes)
        return self.config.schema(extended_table_cls)

    def _create_flag_part_table_classes(self, part_table_classes: Dict[str, Type[Part]]) -> None:
        part_table_classes.update(
            self._create_part_table_classes({name: "-> master" for name in self.config.flag_table_names})
        )

    def _create_non_flag_part_table_classes(self, part_table_classes: Dict[str, Type[Part]]) -> None:
        part_table_classes.update(self._create_part_table_classes(self.config.part_table_definitions))

    def _create_part_table_classes(self, definitions: Dict[str, str]) -> Dict[str, Type[Part]]:
        part_tables = dict()
        for name, definition in definitions.items():
            part_tables[name] = self._create_part_table_cls(name, definition)
        return part_tables

    @staticmethod
    def _create_part_table_cls(name: str, definition: str) -> Type[Part]:
        # noinspection PyTypeChecker
        return type(name, (Part,), dict(definition=definition))
