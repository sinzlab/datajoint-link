"""Contains the DataJoint table factory."""
from __future__ import annotations

import os
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


def create_dj_connection_factory(
    credential_provider: Callable[[], DatabaseServerCredentials]
) -> Callable[[], dj.Connection]:
    """Create a factory producing DataJoint connections."""

    def create_dj_connection() -> dj.Connection:
        return dj.Connection(credential_provider().host, credential_provider().username, credential_provider().password)

    return create_dj_connection


def create_dj_schema_factory(
    name: Callable[[], str], connection_factory: Callable[[], dj.Connection]
) -> Callable[[], dj.Schema]:
    """Create a factory producing DataJoint schemas."""

    def create_dj_schema() -> dj.Schema:
        return dj.Schema(name(), connection=connection_factory())

    return create_dj_schema


class Tiers(Enum):
    """The different DataJoint table tiers."""

    MANUAL = dj.Manual
    LOOKUP = dj.Lookup
    COMPUTED = dj.Computed
    IMPORTED = dj.Imported


@overload
def create_dj_table_factory(name: Callable[[], str], schema_factory: Callable[[], dj.Schema]) -> Callable[[], dj.Table]:
    ...


@overload
def create_dj_table_factory(  # noqa: PLR0913
    name: Callable[[], str],
    schema_factory: Callable[[], dj.Schema],
    *,
    tier: Tiers,
    definition: Callable[[], str],
    parts: Optional[Callable[[], dj.Table]] = None,
    context: Optional[Mapping[str, Callable[[], dj.Table]]] = None,
    replacement_stores: Optional[Mapping[str, str]] = None,
) -> Callable[[], dj.Table]:
    ...


def create_dj_table_factory(  # noqa: PLR0913
    name: Callable[[], str],
    schema_factory: Callable[[], dj.Schema],
    *,
    tier: Optional[Tiers] = None,
    definition: Optional[Callable[[], str]] = None,
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
            return spawned_table_classes[name()]
        except KeyError as exception:
            if tier is None or definition is None:
                raise RuntimeError from exception
            part_definitions: dict[str, str] = {}
            if parts is not None:
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
            processed_definition = replace_stores(definition(), replacement_stores)
            table_cls = type(name(), (tier.value,), {"definition": processed_definition, **part_tables})
            processed_context = {name: factory() for name, factory in context.items()}
            return schema_factory()(table_cls, context=processed_context)()

    return create_dj_table


@dataclass(frozen=True)
class DatabaseServerCredentials:
    """Information necessary to connect to a database server."""

    host: str
    username: str
    password: str


def create_source_credential_provider(host: str) -> Callable[[], DatabaseServerCredentials]:
    """Create an object that provides credentials for the source database server when called."""

    def provide_credentials() -> DatabaseServerCredentials:
        return DatabaseServerCredentials(host, os.environ["LINK_USER"], os.environ["LINK_PASS"])

    return provide_credentials


def create_local_credential_provider() -> Callable[[], DatabaseServerCredentials]:
    """Create an object that provides credentials for the local database server when called."""

    def provide_credentials() -> DatabaseServerCredentials:
        return DatabaseServerCredentials(
            dj.config["database.host"], dj.config["database.user"], dj.config["database.password"]
        )

    return provide_credentials


def create_table_definition_provider(table: Callable[[], dj.Table]) -> Callable[[], str]:
    """Create an object that provides the definition of the table produced by the given factory when called."""

    def provide_definition() -> str:
        return table().describe(printout=False)

    return provide_definition


def create_outbound_table_name_provider(
    source_table_name: str,
    source_credentials: Callable[[], DatabaseServerCredentials],
    local_credentials: Callable[[], DatabaseServerCredentials],
    source_schema_name: str,
    local_schema_name: str,
) -> Callable[[], str]:
    """Create an object that provides the correct link-specific name for the outbound table when called."""

    def provide_name() -> str:
        link_identifiers = [
            source_table_name,
            source_credentials().host,
            local_credentials().host,
            source_schema_name,
            local_schema_name,
        ]
        return "Outbound" + sha1(",".join(link_identifiers).encode()).hexdigest()

    return provide_name


def create_outbound_schema_name_provider() -> Callable[[], str]:
    """Create an object that provides the name of the outbound schema when called."""

    def provide_outbound_schema_name() -> str:
        return os.environ["LINK_OUTBOUND"]

    return provide_outbound_schema_name


def create_dj_link_gateway(
    source_host: str,
    source_schema: str,
    local_schema: str,
    source_table_name: str,
    *,
    replacement_stores: Optional[Mapping[str, str]] = None,
) -> DJLinkGateway:
    """Create a DataJoint link gateway from the given information."""
    source_credential_provider = create_source_credential_provider(source_host)
    source_connection = create_dj_connection_factory(source_credential_provider)
    source_table = create_dj_table_factory(
        lambda: source_table_name, create_dj_schema_factory(lambda: source_schema, source_connection)
    )
    local_credential_provider = create_local_credential_provider()
    outbound_table = create_dj_table_factory(
        create_outbound_table_name_provider(
            source_table_name,
            source_credential_provider,
            local_credential_provider,
            source_schema,
            local_schema,
        ),
        create_dj_schema_factory(create_outbound_schema_name_provider(), source_connection),
        tier=Tiers.MANUAL,
        definition=lambda: "-> source_table",
        context={"source_table": source_table},
    )
    local_table = create_dj_table_factory(
        lambda: source_table_name,
        create_dj_schema_factory(lambda: local_schema, create_dj_connection_factory(local_credential_provider)),
        tier=Tiers.MANUAL,
        definition=create_table_definition_provider(source_table),
        parts=source_table,
        replacement_stores=replacement_stores,
    )
    facade = DJLinkFacade(source_table, outbound_table, local_table)
    return DJLinkGateway(facade, IdentificationTranslator())
