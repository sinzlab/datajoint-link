"""Contains the DataJoint table factory."""
from __future__ import annotations

import functools
from enum import Enum
from typing import Callable, Mapping, Optional, cast, overload

import datajoint as dj

from .config import DatabaseServerCredentials
from .dj_helpers import replace_stores


def create_dj_connection_factory(
    credential_provider: Callable[[], DatabaseServerCredentials]
) -> Callable[[], dj.Connection]:
    """Create a factory producing DataJoint connections."""
    create_cached_connection = functools.lru_cache(dj.Connection)

    def create_dj_connection() -> dj.Connection:
        return create_cached_connection(
            credential_provider().host, credential_provider().username, credential_provider().password
        )

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

    @functools.lru_cache(maxsize=None)
    def create_dj_table() -> dj.Table:
        spawned_table_classes: dict[str, type[dj.Table]] = {}
        schema_factory().spawn_missing_classes(context=spawned_table_classes)
        try:
            return spawned_table_classes[name()]()
        except KeyError as exception:
            if tier is None or definition is None:
                raise RuntimeError from exception
            part_definitions: dict[str, str] = {}
            if parts is not None:
                for child in parts().children(as_objects=True):
                    if not child.table_name.startswith(parts().table_name + "__"):
                        continue
                    part_definition = child.describe(printout=False).replace(parts().full_table_name, "master")
                    part_definitions[dj.utils.to_camel_case(child.table_name.split("__")[-1])] = part_definition
            for part_name, part_definition in part_definitions.items():
                part_definitions[part_name] = replace_stores(part_definition, replacement_stores)
            part_tables: dict[str, type[dj.Part]] = {}
            for part_name, part_definition in part_definitions.items():
                part_tables[part_name] = cast(
                    "type[dj.Part]", type(part_name, (dj.Part,), {"definition": part_definition})
                )
            processed_definition = replace_stores(definition(), replacement_stores)
            table_cls = type(name(), (tier.value,), {"definition": processed_definition, **part_tables})
            processed_context = {name: factory() for name, factory in context.items()}
            return schema_factory()(table_cls, context=processed_context)()

    return create_dj_table
