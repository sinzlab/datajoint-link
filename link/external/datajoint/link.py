import os
from typing import Type, Dict, Optional

from datajoint import Schema, Lookup, AndList
from datajoint.table import Table

from .factory import TableFactory
from .dj_helpers import replace_stores
from ...adapters.datajoint.local_table import LocalTableController
from ...base import Base


class Link(Base):
    _schema_cls = Schema
    _replace_stores_func = staticmethod(replace_stores)
    _table_cls_factories: Dict[str, TableFactory] = None
    _local_table_controller: LocalTableController = None

    def __init__(self, local_schema: Schema, source_schema: Schema, stores: Optional[Dict[str, str]] = None) -> None:
        if stores is None:
            stores = dict()
        self.local_schema = local_schema
        self.source_schema = source_schema
        self.stores = stores

    def __call__(self, table_cls: Type) -> Type[Lookup]:
        self._run_basic_setup(table_cls)
        try:
            return self._table_cls_factories["local"]()
        except RuntimeError:
            self._run_initial_setup()
            return self._table_cls_factories["local"]()

    def _run_basic_setup(self, table_cls: Type) -> None:
        self._run_basic_setup_for_source_table_factory(table_cls)
        self._run_basic_setup_for_outbound_table_factory(table_cls)
        self._run_basic_setup_for_local_table_factory(table_cls)

    def _run_basic_setup_for_source_table_factory(self, table_cls: Type) -> None:
        source_factory = self._table_cls_factories["source"]
        source_factory.schema = self.source_schema
        source_factory.table_name = table_cls.__name__

    def _run_basic_setup_for_outbound_table_factory(self, table_cls: Type) -> None:
        outbound_factory = self._table_cls_factories["outbound"]
        outbound_factory.schema = self._schema_cls(
            os.environ["REMOTE_OUTBOUND_SCHEMA"], connection=self.source_schema.connection
        )
        outbound_factory.table_name = table_cls.__name__ + "Outbound"
        outbound_factory.flag_table_names = ["DeletionRequested", "DeletionApproved"]

    def _run_basic_setup_for_local_table_factory(self, table_cls: Type) -> None:
        local_factory = self._table_cls_factories["local"]
        local_factory.schema = self.local_schema
        local_factory.table_name = table_cls.__name__
        local_factory.table_cls_attrs = dict(controller=self._local_table_controller, pull=pull)
        local_factory.flag_table_names = ["DeletionRequested"]

    def _run_initial_setup(self) -> None:
        source_table_cls = self._table_cls_factories["source"]()
        self._run_initial_setup_for_outbound_table_factory(source_table_cls)
        self._run_initial_setup_for_local_table_factory(source_table_cls)

    def _run_initial_setup_for_outbound_table_factory(self, source_table_cls: Type[Table]) -> None:
        outbound_factory = self._table_cls_factories["outbound"]
        outbound_factory.table_cls_attrs["source_table"] = source_table_cls
        outbound_factory.table_definition = "-> self.source_table"
        outbound_factory()

    def _run_initial_setup_for_local_table_factory(self, source_table_cls: Type[Table]) -> None:
        local_factory = self._table_cls_factories["local"]
        local_factory.table_definition = self._create_definition(source_table_cls)
        local_factory.part_table_definitions = self._create_local_part_table_definitions()

    def _create_local_part_table_definitions(self) -> Dict[str, str]:
        part_table_definitions = dict()
        for name, part in self._table_cls_factories["source"].part_tables.items():
            part_table_definitions[name] = self._create_definition(part)
        return part_table_definitions

    def _create_definition(self, table_cls: Type[Table]) -> str:
        inverted_stores = {source: local for local, source in self.stores.items()}
        return self._replace_stores_func(str(table_cls().heading), inverted_stores)


def pull(self, *restrictions) -> None:
    if not restrictions:
        restrictions = AndList()
    self.controller.pull(restrictions)
