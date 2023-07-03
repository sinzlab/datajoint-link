from __future__ import annotations

import sys
from collections import defaultdict
from collections.abc import Iterable, Mapping
from io import StringIO
from types import TracebackType
from typing import Any, Optional, Protocol, TextIO, Type, TypedDict, cast

from dj_link.adapters.datajoint.facade import DJAssignments, DJProcess
from dj_link.adapters.datajoint.facade import DJLinkFacade as AbstractDJLinkFacade
from dj_link.adapters.datajoint.identification import IdentificationTranslator
from dj_link.custom_types import PrimaryKey
from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import Link, create_link, delete, process, pull
from dj_link.entities.state import Commands, Components, Processes, Update
from dj_link.use_cases.gateway import LinkGateway


class Table(Protocol):
    def insert(self, rows: Iterable[Mapping[str, Any]]) -> None:
        ...

    def insert1(self, row: Mapping[str, Any]) -> None:
        ...

    def fetch(self) -> list[dict[str, Any]]:
        ...

    def fetch1(self) -> dict[str, Any]:
        ...

    def delete(self) -> None:
        ...

    def delete_quick(self) -> None:
        ...

    def proj(self, *attributes: str) -> Table:
        ...

    def __and__(self, condition: PrimaryKey) -> Table:
        ...


class FakeTable:
    def __init__(self, primary: Iterable[str], attrs: Optional[Iterable[str]] = None) -> None:
        self.__primary = set(primary)
        self.__attrs = set(attrs) if attrs is not None else set()
        assert self.__primary.isdisjoint(self.__attrs)
        self.__rows: list[dict[str, Any]] = []
        self.__restricted_attrs: Optional[set[str]] = None
        self.__restriction: Optional[PrimaryKey] = None

    def insert(self, rows: Iterable[Mapping[str, Any]]) -> None:
        for row in rows:
            self.insert1(row)

    def insert1(self, row: Mapping[str, Any]) -> None:
        assert set(row) == self.__primary | self.__attrs
        assert {k: v for k, v in row.items() if k in self.__primary} not in self.proj().fetch()
        self.__rows.append(dict(row))

    def fetch(self) -> list[dict[str, Any]]:
        if self.__restricted_attrs is None:
            restricted_attrs = self.__primary | self.__attrs
        else:
            restricted_attrs = self.__restricted_attrs
        if self.__restriction is not None:
            rows = [row for row in self.__rows if all(row[k] == self.__restriction[k] for k in self.__restriction)]
        else:
            rows = self.__rows
        return [{k: v for k, v in r.items() if k in restricted_attrs} for r in rows]

    def fetch1(self) -> dict[str, Any]:
        return self.fetch()[0]

    def delete(self) -> None:
        def is_confirmed() -> bool:
            answer = None
            while answer not in ("y", "n"):
                answer = input("Really delete? [y/n]: ")
            return answer == "y"

        if not is_confirmed():
            return
        self.delete_quick()

    def delete_quick(self) -> None:
        if self.__restriction is not None:
            indexes = [
                i
                for i, row in enumerate(self.__rows)
                if all(row[k] == self.__restriction[k] for k in self.__restriction)
            ]
        else:
            indexes = list(range(len(self.__rows)))
        for index in indexes:
            del self.__rows[index]

    def proj(self, *attributes: str) -> FakeTable:
        attrs = set(attributes)
        assert attrs <= self.__attrs
        table = FakeTable(attrs=self.__attrs, primary=self.__primary)
        table.__rows = self.__rows
        table.__restricted_attrs = self.__primary | attrs
        table.__restriction = self.__restriction
        return table

    def __and__(self, condition: PrimaryKey) -> FakeTable:
        table = FakeTable(attrs=self.__attrs, primary=self.__primary)
        table.__rows = self.__rows
        table.__restricted_attrs = self.__restricted_attrs
        table.__restriction = condition
        return table


class DJLinkFacade(AbstractDJLinkFacade):
    def __init__(self, source: Table, outbound: Table, local: Table) -> None:
        self.source = source
        self.outbound = outbound
        self.local = local

    def get_assignments(self) -> DJAssignments:
        return DJAssignments(
            cast("list[PrimaryKey]", self.source.proj().fetch()),
            cast("list[PrimaryKey]", self.outbound.proj().fetch()),
            cast("list[PrimaryKey]", self.local.proj().fetch()),
        )

    def get_processes(self) -> list[DJProcess]:
        rows = self.outbound.proj("process").fetch()
        processes: list[DJProcess] = []
        for row in rows:
            process = row.pop("process")
            processes.append(DJProcess(row, process))
        return processes

    def get_tainted_primary_keys(self) -> list[PrimaryKey]:
        rows = [row for row in self.outbound.proj("is_flagged").fetch() if row["is_flagged"] == "TRUE"]
        for row in rows:
            row.pop("is_flagged")
        return cast("list[PrimaryKey]", rows)

    def add_to_local(self, primary_key: PrimaryKey) -> None:
        self.local.insert1((self.source & primary_key).fetch1())

    def remove_from_local(self, primary_key: PrimaryKey) -> None:
        (self.local & primary_key).delete()

    def deprecate(self, primary_key: PrimaryKey) -> None:
        self.__update_row(self.outbound, primary_key, {"process": "NONE", "is_deprecated": "TRUE"})

    def start_pull_process(self, primary_key: PrimaryKey) -> None:
        self.outbound.insert1(dict(primary_key, process="PULL", is_flagged="FALSE", is_deprecated="FALSE"))

    def finish_pull_process(self, primary_key: PrimaryKey) -> None:
        self.__update_row(self.outbound, primary_key, {"process": "NONE"})

    def start_delete_process(self, primary_key: PrimaryKey) -> None:
        self.__update_row(self.outbound, primary_key, {"process": "DELETE"})

    def finish_delete_process(self, primary_key: PrimaryKey) -> None:
        self.__update_row(self.outbound, primary_key, {"process": "NONE"})

    @staticmethod
    def __update_row(table: Table, primary_key: PrimaryKey, changes: Mapping[str, Any]) -> None:
        row = (table & primary_key).fetch1()
        row.update(changes)
        (table & primary_key).delete_quick()
        table.insert1(row)


class DJLinkGateway(LinkGateway):
    def __init__(self, facade: AbstractDJLinkFacade, translator: IdentificationTranslator) -> None:
        self.facade = facade
        self.translator = translator

    def create_link(self) -> Link:
        def translate_assignments(dj_assignments: DJAssignments) -> dict[Components, set[Identifier]]:
            return {
                Components.SOURCE: self.translator.to_identifiers(dj_assignments.source),
                Components.OUTBOUND: self.translator.to_identifiers(dj_assignments.outbound),
                Components.LOCAL: self.translator.to_identifiers(dj_assignments.local),
            }

        def translate_processes(dj_processes: Iterable[DJProcess]) -> dict[Processes, set[Identifier]]:
            persisted_to_domain_process_map = {"PULL": Processes.PULL, "DELETE": Processes.DELETE}
            domain_processes: dict[Processes, set[Identifier]] = defaultdict(set)
            active_processes = [process for process in dj_processes if process.current_process != "NONE"]
            for persisted_process in active_processes:
                domain_process = persisted_to_domain_process_map[persisted_process.current_process]
                domain_processes[domain_process].add(self.translator.to_identifier(persisted_process.primary_key))
            return domain_processes

        def translate_tainted_primary_keys(primary_keys: Iterable[PrimaryKey]) -> set[Identifier]:
            return {self.translator.to_identifier(key) for key in primary_keys}

        return create_link(
            translate_assignments(self.facade.get_assignments()),
            processes=translate_processes(self.facade.get_processes()),
            tainted_identifiers=translate_tainted_primary_keys(self.facade.get_tainted_primary_keys()),
        )

    def apply(self, update: Update) -> None:
        if update.command is Commands.ADD_TO_LOCAL:
            self.facade.add_to_local(self.translator.to_primary_key(update.identifier))
        if update.command is Commands.REMOVE_FROM_LOCAL:
            self.facade.remove_from_local(self.translator.to_primary_key(update.identifier))
        if update.command is Commands.START_PULL_PROCESS:
            self.facade.start_pull_process(self.translator.to_primary_key(update.identifier))
        if update.command is Commands.FINISH_PULL_PROCESS:
            self.facade.finish_pull_process(self.translator.to_primary_key(update.identifier))
        if update.command is Commands.DEPRECATE:
            self.facade.deprecate(self.translator.to_primary_key(update.identifier))
        if update.command is Commands.START_DELETE_PROCESS:
            self.facade.start_delete_process(self.translator.to_primary_key(update.identifier))
        if update.command is Commands.FINISH_DELETE_PROCESS:
            self.facade.finish_delete_process(self.translator.to_primary_key(update.identifier))


class Tables(TypedDict):
    source: FakeTable
    outbound: FakeTable
    local: FakeTable


def create_tables(primary: Iterable[str], non_primary: Iterable[str]) -> Tables:
    return {
        "source": FakeTable(set(primary), set(non_primary)),
        "outbound": FakeTable(set(primary), {"process", "is_flagged", "is_deprecated"}),
        "local": FakeTable(set(primary), set(non_primary)),
    }


def create_gateway(tables: Tables) -> DJLinkGateway:
    facade = DJLinkFacade(**tables)
    translator = IdentificationTranslator()
    return DJLinkGateway(facade, translator)


class State(TypedDict):
    source: list[Mapping[str, Any]]
    outbound: list[Mapping[str, Any]]
    local: list[Mapping[str, Any]]


def set_state(tables: Tables, state: State) -> None:
    tables["source"].insert(state["source"])
    tables["outbound"].insert(state["outbound"])
    tables["local"].insert(state["local"])


def has_state(tables: Tables, expected: State) -> bool:
    return (
        tables["source"].fetch() == expected["source"]
        and tables["outbound"].fetch() == expected["outbound"]
        and tables["local"].fetch() == expected["local"]
    )


class as_stdin:
    def __init__(self, buffer: TextIO) -> None:
        self.buffer = buffer
        self.original_stdin = sys.stdin

    def __enter__(self) -> None:
        sys.stdin = self.buffer

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_value: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> None:
        sys.stdin = self.original_stdin


def test_link_creation() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [
                {"a": 0, "b": 1},
                {"a": 1, "b": 2},
                {"a": 2, "b": 3},
            ],
            "outbound": [
                {"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                {"a": 1, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                {"a": 2, "process": "NONE", "is_flagged": "TRUE", "is_deprecated": "FALSE"},
            ],
            "local": [
                {"a": 2, "b": 3},
                {"a": 0, "b": 1},
            ],
        },
    )

    assert gateway.create_link() == create_link(
        {
            Components.SOURCE: gateway.translator.to_identifiers([{"a": 0}, {"a": 1}, {"a": 2}]),
            Components.OUTBOUND: gateway.translator.to_identifiers([{"a": 0}, {"a": 1}, {"a": 2}]),
            Components.LOCAL: gateway.translator.to_identifiers([{"a": 0}, {"a": 2}]),
        },
        processes={Processes.PULL: {gateway.translator.to_identifier({"a": 1})}},
        tainted_identifiers={gateway.translator.to_identifier({"a": 2})},
    )


def test_add_to_local_command() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [],
        },
    )

    for update in process(gateway.create_link()):
        gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [{"a": 0, "b": 1}],
        },
    )


def test_remove_from_local_command() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [{"a": 0, "b": 1}],
        },
    )

    for update in process(gateway.create_link()):
        with as_stdin(StringIO("y")):
            gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [],
        },
    )


def test_start_pull_process() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(tables, {"source": [{"a": 0, "b": 1}], "outbound": [], "local": []})

    for update in pull(gateway.create_link(), requested={gateway.translator.to_identifier({"a": 0})}):
        gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [],
        },
    )


def test_finish_pull_process() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [{"a": 0, "b": 1}],
        },
    )

    for update in process(gateway.create_link()):
        gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [{"a": 0, "b": 1}],
        },
    )


def test_start_delete_process() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [{"a": 0, "b": 1}],
        },
    )

    for update in delete(gateway.create_link(), requested={gateway.translator.to_identifier({"a": 0})}):
        gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [{"a": 0, "b": 1}],
        },
    )


def test_finish_delete_process() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [],
        },
    )

    for update in process(gateway.create_link()):
        gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}],
            "local": [],
        },
    )


def test_deprecate_process() -> None:
    tables = create_tables(primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "DELETE", "is_flagged": "TRUE", "is_deprecated": "FALSE"}],
            "local": [],
        },
    )

    for update in process(gateway.create_link()):
        gateway.apply(update)

    assert has_state(
        tables,
        {
            "source": [{"a": 0, "b": 1}],
            "outbound": [{"a": 0, "process": "NONE", "is_flagged": "TRUE", "is_deprecated": "TRUE"}],
            "local": [],
        },
    )
