"""Contains the link class that is used by the user to establish a link."""
import os
from typing import Any, Dict, Optional, Type, Union

from datajoint import Lookup, Schema
from datajoint.user_tables import UserTable

from ...base import Base
from ...schemas import LazySchema
from .dj_helpers import replace_stores
from .factory import TableFactory, TableFactoryConfig, TableTiers
from .mixin import LocalTableMixin


class Link(Base):  # pylint: disable=too-few-public-methods
    """Used by the user to establish a link between a source and a local table."""

    schema_cls = Schema
    replace_stores_func = staticmethod(replace_stores)
    base_table_cls: Type[UserTable] = Lookup
    table_cls_factories: Dict[str, TableFactory]

    def __init__(
        self,
        local_schema: Union[Schema, LazySchema],
        source_schema: Union[Schema, LazySchema],
        stores: Optional[Dict[str, str]] = None,
    ) -> None:
        """Initialize the link."""
        if stores is None:
            stores = {}
        self.local_schema = local_schema
        self.source_schema = source_schema
        self.stores = stores

    def __call__(self, table_class: Type) -> Type[UserTable]:
        """Initialize the tables and return the local table."""
        self._configure_table_factory(table_class.__name__, "source")
        self._configure_table_factory(table_class.__name__, "outbound")
        self._configure_table_factory(table_class.__name__, "local")
        try:
            return self.table_cls_factories["local"]()
        except RuntimeError:
            self._configure_table_factory(table_class.__name__, "outbound", initial=True)
            self._configure_table_factory(table_class.__name__, "local", initial=True)
            self.table_cls_factories["outbound"]()
            return self.table_cls_factories["local"]()

    def _configure_table_factory(self, table_name: str, factory_type: str, initial: bool = False) -> None:
        config = self._create_basic_config(table_name, factory_type)
        if initial:
            config = self._create_initial_config(table_name, factory_type)
        self.table_cls_factories[factory_type].config = TableFactoryConfig(**config)

    def _create_basic_config(self, table_name: str, factory_type: str) -> Dict[str, Any]:
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
                bases=(LocalTableMixin,),
                flag_table_names=["DeletionRequested"],
            )
        raise ValueError("Unknown table factory type")

    def _create_initial_config(self, table_name: str, factory_type: str) -> Dict[str, Any]:
        def create_basic_config() -> Dict[str, Any]:
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

            def create_local_part_table_definitions() -> Dict[str, str]:
                return {
                    name: create_definition(part)
                    for name, part in self.table_cls_factories["source"].part_tables.items()
                }

            def create_definition(table_cls: Type[UserTable]) -> str:
                return self.replace_stores_func(str(table_cls().heading), self.stores)

            return dict(
                create_basic_config(),
                definition=create_definition(self.table_cls_factories["source"]()),
                part_table_definitions=create_local_part_table_definitions(),
                tier=TableTiers.LOOKUP,
            )
        raise ValueError("Unknown table factory type")
