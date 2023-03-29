"""Contains the link class that is used by the user to establish a link."""
from __future__ import annotations

import os
from typing import Any, Optional, Union

from datajoint import Schema
from datajoint.user_tables import UserTable

from ...adapters.datajoint import initialize_adapters
from ...adapters.datajoint.controller import Controller
from ...base import Base
from ...globals import REPOSITORY_NAMES
from ...schemas import LazySchema
from ...use_cases import REQUEST_MODELS, USE_CASES, initialize_use_cases
from . import TableFacadeLink
from .dj_helpers import replace_stores
from .facade import TableFacade
from .factory import TableFactory, TableFactoryConfig, TableTiers
from .file import ReusableTemporaryDirectory
from .mixin import LocalTableMixin, create_local_table_mixin_class
from .printer import Printer


def initialize() -> None:
    """Initialize the system."""
    temp_dir = ReusableTemporaryDirectory("link_")
    factories = {facade_type: TableFactory() for facade_type in REPOSITORY_NAMES}
    facades = {facade_type: TableFacade(table_factory, temp_dir) for facade_type, table_factory in factories.items()}
    facade_link = TableFacadeLink(**facades)
    gateway_link, view_model, presenter = initialize_adapters(facade_link)
    output_ports = {name: getattr(presenter, name) for name in USE_CASES}
    initialized_use_cases = initialize_use_cases(gateway_link, output_ports)

    local_table_mixin = create_local_table_mixin_class()
    local_table_mixin.temp_dir = temp_dir
    local_table_mixin.source_table_factory = factories["source"]
    local_table_mixin.controller = Controller(initialized_use_cases, REQUEST_MODELS, gateway_link)
    local_table_mixin.printer = Printer(view_model)

    Link.table_cls_factories = factories
    Link.local_table_mixin = local_table_mixin


class Link(Base):  # pylint: disable=too-few-public-methods
    """Used by the user to establish a link between a source and a local table."""

    schema_cls = Schema
    replace_stores_func = staticmethod(replace_stores)
    table_cls_factories: dict[str, TableFactory]
    local_table_mixin: type[LocalTableMixin]

    def __init__(
        self,
        local_schema: Union[Schema, LazySchema],
        source_schema: Union[Schema, LazySchema],
        stores: Optional[dict[str, str]] = None,
    ) -> None:
        """Initialize the link."""
        if stores is None:
            stores = {}
        self.local_schema = local_schema
        self.source_schema = source_schema
        self.stores = stores

    def __call__(self, table_class: type) -> type[UserTable]:
        """Initialize the tables and return the local table."""
        return self._create_local_table_class(table_class.__name__)

    def _create_local_table_class(self, table_name: str) -> type[UserTable]:
        self._configure_table_factories(table_name)
        try:
            return self.table_cls_factories["local"]()
        except RuntimeError:
            self._configure_table_factories(table_name, initial=True)
            return self.table_cls_factories["local"]()

    def _configure_table_factories(self, table_name: str, *, initial: bool = False) -> None:
        self._configure_table_factory(table_name, "source", initial=initial)
        self._configure_table_factory(table_name, "outbound", initial=initial)
        self._configure_table_factory(table_name, "local", initial=initial)

    def _configure_table_factory(self, table_name: str, factory_type: str, initial: bool = False) -> None:
        config = self._create_basic_config(table_name, factory_type)
        if initial:
            config = self._create_initial_config(table_name, factory_type)
        self.table_cls_factories[factory_type].config = TableFactoryConfig(**config)

    def _create_basic_config(self, table_name: str, factory_type: str) -> dict[str, Any]:
        if factory_type == "source":
            return dict(schema=self.source_schema, name=table_name)
        if factory_type == "outbound":
            return dict(
                schema=self.schema_cls(os.environ["LINK_OUTBOUND"], connection=self.source_schema.connection),
                name=table_name + "Outbound",
                flag_table_names=["DeletionRequested", "DeletionApproved"],
            )
        if factory_type == "local":
            return dict(
                schema=self.local_schema,
                name=table_name,
                bases=(self.local_table_mixin,),
                flag_table_names=["DeletionRequested"],
            )
        raise ValueError("Unknown table factory type")

    def _create_initial_config(self, table_name: str, factory_type: str) -> dict[str, Any]:
        def create_basic_config() -> dict[str, Any]:
            return self._create_basic_config(table_name, factory_type)

        if factory_type == "source":
            return create_basic_config()
        if factory_type == "outbound":
            return dict(
                create_basic_config(),
                definition="-> source_table",
                context={"source_table": self.table_cls_factories["source"]()},
                tier=TableTiers.LOOKUP,
            )
        if factory_type == "local":

            def create_local_part_table_definitions() -> dict[str, str]:
                return {
                    name: create_definition(part)
                    for name, part in self.table_cls_factories["source"].part_tables.items()
                }

            def create_definition(table_cls: type[UserTable]) -> str:
                return self.replace_stores_func(str(table_cls().heading), self.stores)

            return dict(
                create_basic_config(),
                definition=create_definition(self.table_cls_factories["source"]()),
                part_table_definitions=create_local_part_table_definitions(),
                tier=TableTiers.LOOKUP,
            )
        raise ValueError("Unknown table factory type")
