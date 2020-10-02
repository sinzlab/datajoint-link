import os
from typing import Type, Dict, Optional, Any, Union

from datajoint import Schema, Lookup
from datajoint.user_tables import UserTable

from .factory import TableFactoryConfig, TableFactory
from .dj_helpers import replace_stores
from .mixin import LocalTableMixin
from ...base import Base
from ...schemas import LazySchema


class Link(Base):
    _schema_cls = Schema
    _replace_stores_func = staticmethod(replace_stores)
    _base_table_cls: Type[UserTable] = Lookup
    _table_cls_factories: Dict[str, TableFactory]

    def __init__(
        self,
        local_schema: Union[Schema, LazySchema],
        source_schema: Union[Schema, LazySchema],
        stores: Optional[Dict[str, str]] = None,
    ) -> None:
        if stores is None:
            stores = {}
        self.local_schema = local_schema
        self.source_schema = source_schema
        self.stores = stores

    def __call__(self, cls: Type) -> Type[UserTable]:
        self._configure(cls, "source")
        self._configure(cls, "outbound")
        self._configure(cls, "local")
        try:
            return self._table_cls_factories["local"]()
        except RuntimeError:
            self._configure(cls, "outbound", initial=True)
            self._configure(cls, "local", initial=True)
            self._table_cls_factories["outbound"]()
            return self._table_cls_factories["local"]()

    def _configure(self, table_cls: Type, factory_type: str, initial: bool = False) -> None:
        config = self._create_basic_config(table_cls, factory_type)
        if initial:
            config = self._create_initial_config(table_cls, factory_type)
        self._table_cls_factories[factory_type].config = TableFactoryConfig(**config)

    def _create_basic_config(self, table_cls: Type, factory_type: str) -> Dict[str, Any]:
        if factory_type == "source":
            return dict(schema=self.source_schema, table_name=table_cls.__name__)
        elif factory_type == "outbound":
            return dict(
                schema=self._schema_cls(os.environ["LINK_OUTBOUND"], connection=self.source_schema.connection),
                table_name=table_cls.__name__ + "Outbound",
                flag_table_names=["DeletionRequested", "DeletionApproved"],
            )
        else:
            return dict(
                schema=self.local_schema,
                table_name=table_cls.__name__,
                table_bases=(LocalTableMixin,),
                flag_table_names=["DeletionRequested"],
            )

    def _create_initial_config(self, table_cls: Type, factory_type: str) -> Dict[str, Any]:
        config = self._create_basic_config(table_cls, factory_type)
        if factory_type == "outbound":
            return dict(
                config,
                table_cls=self._base_table_cls,
                table_cls_attrs=dict(source_table=self._table_cls_factories["source"]()),
                table_definition="-> self.source_table",
            )
        else:
            return dict(
                config,
                table_cls=self._base_table_cls,
                table_definition=self._create_definition(self._table_cls_factories["source"]()),
                part_table_definitions=self._create_local_part_table_definitions(),
            )

    def _create_local_part_table_definitions(self) -> Dict[str, str]:
        part_table_definitions = {}
        for name, part in self._table_cls_factories["source"].part_tables.items():
            part_table_definitions[name] = self._create_definition(part)
        return part_table_definitions

    def _create_definition(self, table_cls: Type[UserTable]) -> str:
        return self._replace_stores_func(str(table_cls().heading), self.stores)
