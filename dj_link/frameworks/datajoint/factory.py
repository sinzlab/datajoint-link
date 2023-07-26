"""Contains the DataJoint table factory."""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from hashlib import sha1
from typing import Any, Callable, Collection, Dict, Mapping, Optional, Tuple, Type, overload

import datajoint as dj
from datajoint import Computed, Imported, Lookup, Manual, Part, Schema
from datajoint.user_tables import UserTable

from dj_link.adapters.datajoint.gateway import DJLinkGateway
from dj_link.adapters.datajoint.identification import IdentificationTranslator

from ...base import Base
from .dj_helpers import get_part_table_classes, replace_stores
from .facade import DJLinkFacade


class TableTiers(Enum):
    """Table tiers that can be used in the table factory."""

    MANUAL = Manual
    LOOKUP = Lookup
    COMPUTED = Computed
    IMPORTED = Imported


@dataclass
class TableFactoryConfig:  # pylint: disable=too-many-instance-attributes
    """Configuration used by the table factory to spawn/create tables."""

    schema: Schema
    name: str
    bases: Tuple[Type, ...] = field(default_factory=tuple)
    flag_table_names: Collection[str] = field(default_factory=list)
    tier: Optional[TableTiers] = None
    definition: Optional[str] = None
    context: Mapping[str, Any] = field(default_factory=dict)
    part_table_definitions: Mapping[str, str] = field(default_factory=dict)

    @property
    def is_table_creation_possible(self) -> bool:
        """Return True if the configuration object contains the information necessary for table creation."""
        return bool(self.tier) and bool(self.definition)


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
            return type(self.config.name, self.config.bases + (table_cls,), {})

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
        table_cls = spawned_table_classes[self.config.name]
        return table_cls

    def _create_table_cls(self) -> Type[UserTable]:
        def create_part_table_classes() -> Dict[str, Type[Part]]:
            def create_part_table_classes(definitions: Mapping[str, str]) -> Dict[str, Type[Part]]:
                def create_part_table_class(name: str, definition: str) -> Type[Part]:
                    return type(name, (Part,), {"definition": definition})

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
            assert self.config.tier is not None, "No table tier specified"
            return type(
                self.config.name,
                (self.config.tier.value,),
                {"definition": self.config.definition, **create_part_table_classes()},
            )

        assert self.config.definition is not None, "No table definition present"
        return self.config.schema(derive_table_class(), context=self.config.context)


def create_dj_connection_factory(host: str, username: str, password: str) -> Callable[[], dj.Connection]:
    """Create a factory producing DataJoint connections."""

    def create_dj_connection() -> dj.Connection:
        return dj.Connection(host, username, password)

    return create_dj_connection


def create_dj_schema_factory(name: str, connection_factory: Callable[[], dj.Connection]) -> Callable[[], dj.Schema]:
    """Create a factory producing DataJoint schemas."""

    def create_dj_schema() -> dj.Schema:
        return dj.Schema(name, connection=connection_factory())

    return create_dj_schema


class Tiers(Enum):
    """The different DataJoint table tiers."""

    MANUAL = dj.Manual
    LOOKUP = dj.Lookup
    COMPUTED = dj.Computed
    IMPORTED = dj.Imported


@overload
def create_dj_table_factory(name: str, schema_factory: Callable[[], dj.Schema]) -> Callable[[], dj.Table]:
    ...


@overload
def create_dj_table_factory(  # noqa: PLR0913
    name: str,
    schema_factory: Callable[[], dj.Schema],
    *,
    tier: Tiers,
    definition: str | Callable[[], dj.Table],
    parts: Optional[Callable[[], dj.Table]] = None,
    context: Optional[Mapping[str, Callable[[], dj.Table]]] = None,
    replacement_stores: Optional[Mapping[str, str]] = None,
) -> Callable[[], dj.Table]:
    ...


def create_dj_table_factory(  # noqa: PLR0913
    name: str,
    schema_factory: Callable[[], dj.Schema],
    *,
    tier: Optional[Tiers] = None,
    definition: Optional[str | Callable[[], dj.Table]] = None,
    parts: Optional[Callable[[], dj.Table]] = None,
    context: Optional[Mapping[str, Callable[[], dj.Table]]] = None,
    replacement_stores: Optional[Mapping[str, str]] = None,
) -> Callable[[], dj.Table]:
    """Create a factory that produces DataJoint tables."""
    if replacement_stores is None:
        replacement_stores = {}
    if context is None:
        context = {}

    def create_dj_table() -> dj.Table:
        spawned_table_classes: dict[str, dj.Table] = {}
        schema_factory().spawn_missing_classes(context=spawned_table_classes)
        try:
            return spawned_table_classes[name]
        except KeyError as exception:
            if tier is None or definition is None:
                raise RuntimeError from exception
            if parts is not None:
                part_definitions: dict[str, str] = {}
                for child in parts().children(as_objects=True):
                    if not child.table_name.startswith(parts().table_name + "__"):
                        continue
                    part_definition = child.describe(printout=False).replace(parts().full_table_name, "master")
                    part_definitions[dj.utils.to_camel_case(child.table_name.split("__")[1])] = part_definition
            for part_name, part_definition in part_definitions.items():
                part_definitions[part_name] = replace_stores(part_definition, replacement_stores)
            part_tables: dict[str, dj.Part] = {}
            for part_name, part_definition in part_definitions.items():
                part_tables[part_name] = type(part_name, (dj.Part,), {"definition": part_definition})
            if callable(definition):
                processed_definition = definition().describe(printout=False)
            else:
                processed_definition = definition
            processed_definition = replace_stores(processed_definition, replacement_stores)
            table_cls = type(name, (tier.value,), {"definition": processed_definition, **part_tables})
            processed_context = {name: factory() for name, factory in context.items()}
            return schema_factory()(table_cls, context=processed_context)()

    return create_dj_table


@dataclass(frozen=True)
class ConnectionDetails:
    """Information necessary to connect to a database server."""

    host: str
    username: str
    password: str


@dataclass(frozen=True)
class SchemaNames:
    """The names of the schemas involved in link."""

    source: str
    outbound: str
    local: str


def create_dj_link_gateway(
    source_connection_details: ConnectionDetails,
    local_connection_details: ConnectionDetails,
    schema_names: SchemaNames,
    source_table_name: str,
    *,
    replacement_stores: Optional[Mapping[str, str]] = None,
) -> DJLinkGateway:
    """Create a DataJoint link gateway from the given information."""
    source_connection = create_dj_connection_factory(
        source_connection_details.host, source_connection_details.username, source_connection_details.password
    )
    source_table = create_dj_table_factory(
        source_table_name,
        create_dj_schema_factory(schema_names.source, source_connection),
    )
    link_identifiers = [
        source_connection_details.host,
        schema_names.source,
        source_table_name,
        local_connection_details.host,
        schema_names.local,
    ]
    outbound_table_name = "Outbound" + sha1(",".join(link_identifiers).encode()).hexdigest()
    outbound_table = create_dj_table_factory(
        outbound_table_name,
        create_dj_schema_factory(schema_names.outbound, source_connection),
        tier=Tiers.MANUAL,
        definition="-> source_table",
        context={"source_table": source_table},
    )
    local_table = create_dj_table_factory(
        source_table_name,
        create_dj_schema_factory(
            schema_names.local,
            create_dj_connection_factory(
                local_connection_details.host, local_connection_details.username, local_connection_details.password
            ),
        ),
        tier=Tiers.MANUAL,
        definition=source_table,
        parts=source_table,
        replacement_stores=replacement_stores,
    )
    facade = DJLinkFacade(source_table, outbound_table, local_table)
    return DJLinkGateway(facade, IdentificationTranslator())
