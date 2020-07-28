from typing import Type, Dict, Optional, Union

from datajoint import Schema, Lookup
from datajoint.table import Table

from .factory import TableFactory, SpawnTableConfig, CreateTableConfig
from ..dj_helpers import replace_stores
from ...adapters.datajoint.local_table import LocalTableController
from ...entities.representation import represent


class Link:
    _schema_cls = Schema
    _replace_stores_func = replace_stores
    _table_cls_factories: Dict[str, TableFactory] = None
    _local_table_controller: LocalTableController = None

    def __init__(self, local_schema: Schema, source_schema: Schema, stores: Optional[Dict[str, str]] = None) -> None:
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
        self._table_cls_factories["source"].spawn_table_config = SpawnTableConfig(
            self.source_schema, table_cls.__name__, dict(), list()
        )

    def _run_basic_setup_for_outbound_table_factory(self, table_cls: Type) -> None:
        self._table_cls_factories["outbound"].spawn_table_config = SpawnTableConfig(
            self._schema_cls("datajoint_outbound__" + self.source_schema.database),
            table_cls.__name__ + "Outbound",
            dict(),
            ["DeletionRequested", "DeletionApproved"],
        )

    def _run_basic_setup_for_local_table_factory(self, table_cls: Type) -> None:
        self._table_cls_factories["local"].spawn_table_config = SpawnTableConfig(
            self.local_schema,
            table_cls.__name__,
            dict(controller=self._local_table_controller, pull=pull),
            ["DeletionRequested"],
        )

    def _run_initial_setup(self) -> None:
        source_table_cls = self._table_cls_factories["source"]()
        self._run_initial_setup_for_outbound_table_factory(source_table_cls)
        self._run_initial_setup_for_local_table_factory(source_table_cls)

    def _run_initial_setup_for_outbound_table_factory(self, source_table_cls: Type[Table]) -> None:
        self._table_cls_factories["outbound"].spawn_table_config.table_cls_attrs["source_table"] = source_table_cls
        self._table_cls_factories["outbound"].create_table_config = CreateTableConfig("-> self.source_table", dict())
        self._table_cls_factories["outbound"]()

    def _run_initial_setup_for_local_table_factory(self, source_table_cls: Type[Table]) -> None:
        self._table_cls_factories["local"].create_table_config = CreateTableConfig(
            **self._create_local_table_definitions(source_table_cls)
        )

    def _create_local_table_definitions(self, source_table_cls: Type[Table]) -> Dict[str, Union[str, Dict[str, str]]]:
        return dict(
            table_definition=self._create_definition(source_table_cls),
            part_table_definitions=self._create_local_part_table_definitions(),
        )

    def _create_local_part_table_definitions(self) -> Dict[str, str]:
        part_table_definitions = dict()
        for name, part in self._table_cls_factories["source"].part_tables.items():
            part_table_definitions[name] = self._create_definition(part)
        return part_table_definitions

    def _create_definition(self, table_cls: Type[Table]) -> str:
        return self._replace_stores_func(str(table_cls().heading), self.stores)

    def __repr__(self) -> str:
        return represent(self, ["local_schema", "source_schema", "stores"])


def pull(self, *restrictions) -> None:
    self.controller.pull(restrictions)
