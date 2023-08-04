"""Contains code gluing the adapters to DataJoint."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field

import datajoint as dj

from ...adapters.datajoint import AbstractTableFacadeLink
from ...base import Base
from .config import (
    create_local_credential_provider,
    create_outbound_schema_name_provider,
    create_outbound_table_name_provider,
    create_source_credential_provider,
    create_table_definition_provider,
)
from .facade import TableFacade
from .factory import Tiers, create_dj_connection_factory, create_dj_schema_factory, create_dj_table_factory


class TableFacadeLink(AbstractTableFacadeLink, Base):
    """Contains the three DataJoint table facades corresponding to the three table types."""

    def __init__(self, source: TableFacade, outbound: TableFacade, local: TableFacade) -> None:
        """Initialize the DataJoint table facade link."""
        self._source = source
        self._outbound = outbound
        self._local = local

    @property
    def source(self) -> TableFacade:
        """Return the source table facade."""
        return self._source

    @property
    def outbound(self) -> TableFacade:
        """Return the outbound table facade."""
        return self._outbound

    @property
    def local(self) -> TableFacade:
        """Return the local table facade."""
        return self._local


@dataclass(frozen=True)
class DJConfiguration:
    """Information needed to configure the DataJoint backend."""

    source_host: str
    source_schema: str
    local_schema: str
    source_table_name: str
    replacement_stores: Mapping[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class DJTables:
    """The three DataJoint tables involved in a link."""

    source: Callable[[], dj.Table]
    outbound: Callable[[], dj.Table]
    local: Callable[[], dj.Table]


def create_tables(config: DJConfiguration) -> DJTables:
    """Create a DataJoint link gateway from the given information."""
    source_credential_provider = create_source_credential_provider(config.source_host)
    source_connection = create_dj_connection_factory(source_credential_provider)
    source_table = create_dj_table_factory(
        lambda: config.source_table_name, create_dj_schema_factory(lambda: config.source_schema, source_connection)
    )
    local_credential_provider = create_local_credential_provider()
    outbound_table = create_dj_table_factory(
        create_outbound_table_name_provider(
            config.source_table_name,
            source_credential_provider,
            local_credential_provider,
            config.source_schema,
            config.local_schema,
        ),
        create_dj_schema_factory(create_outbound_schema_name_provider(), source_connection),
        tier=Tiers.MANUAL,
        definition=lambda: "\n".join(
            [
                "-> source_table",
                "---",
                "process: enum('PULL', 'DELETE', 'NONE')",
                "is_flagged: enum('TRUE', 'FALSE')",
                "is_deprecated: enum('TRUE', 'FALSE')",
            ]
        ),
        context={"source_table": source_table},
    )
    local_table = create_dj_table_factory(
        lambda: config.source_table_name,
        create_dj_schema_factory(lambda: config.local_schema, create_dj_connection_factory(local_credential_provider)),
        tier=Tiers.MANUAL,
        definition=create_table_definition_provider(source_table),
        parts=source_table,
        replacement_stores=config.replacement_stores,
    )
    return DJTables(source_table, outbound_table, local_table)
