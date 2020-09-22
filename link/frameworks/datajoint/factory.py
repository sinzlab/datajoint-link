from __future__ import annotations
from typing import Optional, Dict, Type, Any, Tuple, Mapping, Collection, MutableMapping
from dataclasses import dataclass, field

from datajoint import Schema, Part
from datajoint.user_tables import UserTable

from .dj_helpers import get_part_table_classes
from ...base import Base


@dataclass
class TableFactoryConfig:
    """Configuration used by the table factory to spawn/create tables."""

    schema: Schema
    table_name: str
    table_bases: Tuple[Type, ...] = field(default_factory=tuple)
    table_cls_attrs: Mapping[str, Any] = field(default_factory=dict)
    flag_table_names: Collection[str] = field(default_factory=list)
    table_cls: Optional[Type[UserTable]] = None
    table_definition: Optional[str] = None
    part_table_definitions: Mapping[str, str] = field(default_factory=dict)

    @property
    def is_table_creation_possible(self) -> bool:
        """Returns True if the configuration object contains the information necessary for table creation."""
        return bool(self.table_cls) and bool(self.table_definition)


class TableFactory(Base):
    """Factory that creates table classes according to a provided configuration object."""

    def __init__(self) -> None:
        self._config: Optional[TableFactoryConfig] = None

    @property
    def config(self) -> TableFactoryConfig:
        if self._config is None:
            raise RuntimeError("Config is not set")
        return self._config

    @config.setter
    def config(self, config: TableFactoryConfig) -> None:
        self._config = config

    def __call__(self) -> Type[UserTable]:
        """Spawns or creates (if spawning fails) the table class according to the configuration object."""
        try:
            table_cls = self._spawn_table_cls()
        except KeyError:
            if not self.config.is_table_creation_possible:
                raise RuntimeError("Table could neither be spawned nor created")
            table_cls = self._create_table_cls()
        return table_cls

    @property
    def part_tables(self) -> Dict[str, Type[Part]]:
        """Returns all non-flag part table classes associated with the table class."""
        return get_part_table_classes(self(), ignored_parts=self.config.flag_table_names)

    @property
    def flag_tables(self) -> Dict[str, Type[Part]]:
        """Returns all part table classes associated with the table class."""
        return {name: getattr(self(), name) for name in self.config.flag_table_names}

    def _spawn_table_cls(self) -> Type[UserTable]:
        spawned_table_classes: Dict[str, Type[UserTable]] = {}
        self.config.schema.spawn_missing_classes(context=spawned_table_classes)
        table_cls = spawned_table_classes[self.config.table_name]
        return self._extend_table_cls(table_cls)

    def _extend_table_cls(
        self, table_cls: Type[UserTable], part_table_classes: Optional[Mapping[str, Type[Part]]] = None
    ) -> Type[UserTable]:
        if part_table_classes is None:
            part_table_classes = {}
        if self.config.table_definition:
            table_cls_attrs = dict(self.config.table_cls_attrs, definition=self.config.table_definition)
        else:
            table_cls_attrs = dict(self.config.table_cls_attrs)
        # noinspection PyTypeChecker
        return type(
            self.config.table_name, self.config.table_bases + (table_cls,), {**table_cls_attrs, **part_table_classes}
        )

    def _create_table_cls(self) -> Type[UserTable]:
        part_table_classes: Dict[str, Type[Part]] = {}
        self._create_flag_part_table_classes(part_table_classes)
        self._create_non_flag_part_table_classes(part_table_classes)
        assert self.config.table_cls is not None
        extended_table_cls = self._extend_table_cls(self.config.table_cls, part_table_classes)
        return self.config.schema(extended_table_cls)

    def _create_flag_part_table_classes(self, part_table_classes: MutableMapping[str, Type[Part]]) -> None:
        part_table_classes.update(
            self._create_part_table_classes({name: "-> master" for name in self.config.flag_table_names})
        )

    def _create_non_flag_part_table_classes(self, part_table_classes: MutableMapping[str, Type[Part]]) -> None:
        part_table_classes.update(self._create_part_table_classes(self.config.part_table_definitions))

    def _create_part_table_classes(self, definitions: Mapping[str, str]) -> Dict[str, Type[Part]]:
        part_tables = {}
        for name, definition in definitions.items():
            part_tables[name] = self._create_part_table_cls(name, definition)
        return part_tables

    @staticmethod
    def _create_part_table_cls(name: str, definition: str) -> Type[Part]:
        # noinspection PyTypeChecker
        return type(name, (Part,), dict(definition=definition))
