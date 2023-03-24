"""Contains the DataJoint table factory."""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Collection, Dict, Mapping, Optional, Tuple, Type

from datajoint import Part, Schema
from datajoint.user_tables import UserTable

from ...base import Base
from .dj_helpers import get_part_table_classes


@dataclass
class TableFactoryConfig:  # pylint: disable=too-many-instance-attributes
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
        """Return True if the configuration object contains the information necessary for table creation."""
        return bool(self.table_cls) and bool(self.table_definition)


class TableFactory(Base):
    """Factory that creates table classes according to a provided configuration object."""

    def __init__(self) -> None:
        """Initialize the table factory."""
        self._config: Optional[TableFactoryConfig] = None

    @property
    def config(self) -> TableFactoryConfig:
        """Return the configuration of the table factory or raise an error if it is not set."""
        if self._config is None:
            raise RuntimeError("Config is not set")
        return self._config

    @config.setter
    def config(self, config: TableFactoryConfig) -> None:
        self._config = config

    def __call__(self) -> Type[UserTable]:
        """Spawn or create (if spawning fails) the table class according to the configuration object."""

        def extend_table_cls(table_cls: Type[UserTable]) -> Type[UserTable]:
            return type(
                self.config.table_name,
                self.config.table_bases + (table_cls,),
                dict(self.config.table_cls_attrs),
            )

        try:
            table_cls = self._spawn_table_cls()
        except KeyError as error:
            if not self.config.is_table_creation_possible:
                raise RuntimeError("Table could neither be spawned nor created") from error
            table_cls = self._create_table_cls()
        return extend_table_cls(table_cls)

    @property
    def part_tables(self) -> Dict[str, Type[Part]]:
        """Return all non-flag part table classes associated with the table class."""
        return get_part_table_classes(self(), ignored_parts=self.config.flag_table_names)

    @property
    def flag_tables(self) -> Dict[str, Type[Part]]:
        """Return all part table classes associated with the table class."""
        return {name: getattr(self(), name) for name in self.config.flag_table_names}

    def _spawn_table_cls(self) -> Type[UserTable]:
        spawned_table_classes: Dict[str, Type[UserTable]] = {}
        self.config.schema.spawn_missing_classes(context=spawned_table_classes)
        table_cls = spawned_table_classes[self.config.table_name]
        return table_cls

    def _create_table_cls(self) -> Type[UserTable]:
        def create_part_table_classes() -> Dict[str, Type[Part]]:
            def create_part_table_classes(definitions: Mapping[str, str]) -> Dict[str, Type[Part]]:
                def create_part_table_class(name: str, definition: str) -> Type[Part]:
                    return type(name, (Part,), dict(definition=definition))

                return {name: create_part_table_class(name, definition) for name, definition in definitions.items()}

            def create_flag_part_table_classes() -> Dict[str, Type[Part]]:
                return create_part_table_classes({name: "-> master" for name in self.config.flag_table_names})

            def create_non_flag_part_table_classes() -> Dict[str, Type[Part]]:
                return create_part_table_classes(self.config.part_table_definitions)

            part_table_classes: Dict[str, Type[Part]] = {}
            part_table_classes.update(create_flag_part_table_classes())
            part_table_classes.update(create_non_flag_part_table_classes())
            return part_table_classes

        def derive_table_class() -> Type[UserTable]:
            assert self.config.table_cls is not None, "No table class present"
            return type(
                self.config.table_name,
                (self.config.table_cls,),
                dict(definition=self.config.table_definition, **create_part_table_classes()),
            )

        assert self.config.table_definition is not None, "No table definition present"
        return self.config.schema(derive_table_class())
