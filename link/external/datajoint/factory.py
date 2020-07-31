from __future__ import annotations
from typing import Optional, List, Dict, Type, Any, Union

from datajoint import Schema, Lookup, Part, Table

from .dj_helpers import get_part_table_classes
from ...entities.representation import Base


class TableFactory(Base):
    def __init__(self) -> None:
        self.schema: Optional[Schema] = None
        self.table_name: Optional[str] = None
        self.table_cls_attrs: Dict[str, Any] = dict()
        self.flag_table_names: List[str] = list()
        self.table_definition: Optional[str] = None
        self.part_table_definitions: Dict[str, str] = dict()

    def __call__(self) -> Union[Type[Lookup], Type[Table]]:
        if self.schema is None or self.table_name is None:
            raise RuntimeError
        try:
            table_cls = self._spawn_table_cls()
        except KeyError:
            if self.table_definition is None:
                raise RuntimeError
            table_cls = self._create_table_cls()
        return table_cls

    @property
    def part_tables(self) -> Dict[str, Type[Part]]:
        return get_part_table_classes(self(), ignored_parts=self.flag_table_names)

    @property
    def flag_tables(self) -> Dict[str, Type[Part]]:
        return {name: getattr(self(), name) for name in self.flag_table_names}

    def _spawn_table_cls(self) -> Union[Type[Lookup], Type[Table]]:
        spawned_table_classes = dict()
        self.schema.spawn_missing_classes(context=spawned_table_classes)
        table_cls = spawned_table_classes[self.table_name]
        self._set_attrs_on_table_cls(table_cls)
        return table_cls

    def _create_table_cls(self) -> Type[Lookup]:
        table_cls = self._create_master_table_cls()
        table_cls_attrs = self._create_table_cls_attrs()
        self._set_attrs_on_table_cls(table_cls, table_cls_attrs)
        return self.schema(table_cls)

    def _create_master_table_cls(self) -> Type[Lookup]:
        # noinspection PyTypeChecker
        return type(self.table_name, (Lookup,), dict(definition=self.table_definition),)

    def _create_table_cls_attrs(self) -> Dict[str, Type[Part]]:
        table_cls_attrs = dict()
        self._create_flag_table_classes(table_cls_attrs)
        self._create_non_flag_part_table_classes(table_cls_attrs)
        return table_cls_attrs

    def _create_flag_table_classes(self, table_cls_attrs: Dict[str, Type[Part]]) -> None:
        table_cls_attrs.update(self._create_part_table_classes({name: "-> master" for name in self.flag_table_names}))

    def _create_non_flag_part_table_classes(self, table_cls_attrs: Dict[str, Type[Part]]) -> None:
        table_cls_attrs.update(self._create_part_table_classes(self.part_table_definitions))

    def _create_part_table_classes(self, definitions: Dict[str, str]) -> Dict[str, Type[Part]]:
        part_tables = dict()
        for name, definition in definitions.items():
            part_tables[name] = self._create_part_table_cls(name, definition)
        return part_tables

    @staticmethod
    def _create_part_table_cls(name: str, definition: str) -> Type[Part]:
        # noinspection PyTypeChecker
        return type(name, (Part,), dict(definition=definition))

    def _set_attrs_on_table_cls(self, table_cls: Type, additional_table_cls_attrs: Optional[Dict[str, Any]] = None):
        table_cls_attrs = self.table_cls_attrs
        if additional_table_cls_attrs:
            table_cls_attrs = {**table_cls_attrs, **additional_table_cls_attrs}
        for attr_name, attr_value in table_cls_attrs.items():
            setattr(table_cls, attr_name, attr_value)
