"""Contains code related to configuring a DataJoint-backed link."""
from __future__ import annotations

import os
from collections.abc import Callable
from dataclasses import dataclass
from hashlib import sha1

import datajoint as dj


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
