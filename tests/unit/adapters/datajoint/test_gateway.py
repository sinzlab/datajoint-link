from __future__ import annotations

import os
import re
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from io import StringIO
from itertools import groupby
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Optional, Protocol, TextIO, Type, TypedDict, Union, cast

import pytest

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

    def fetch(self, download_path: str = ...) -> list[dict[str, Any]]:
        ...

    def delete(self) -> None:
        ...

    def delete_quick(self) -> None:
        ...

    def proj(self, *attributes: str) -> Table:
        ...

    def __and__(self, condition: Union[str, PrimaryKey, Iterable[PrimaryKey]]) -> Table:
        ...

    def children(self, *, as_objects: Literal[True]) -> Sequence[Table]:
        ...

    @property
    def table_name(self) -> str:
        ...


class FakeTable:
    def __init__(
        self,
        name: str,
        primary: Iterable[str],
        attrs: Optional[Iterable[str]] = None,
        children: Optional[Iterable[FakeTable]] = None,
        external_attrs: Optional[Iterable[str]] = None,
    ) -> None:
        self.__name = name
        self.__primary = set(primary)
        self.__attrs = set(attrs) if attrs is not None else set()
        self.__children = list(children) if children is not None else list()
        self.__external_attrs = set(external_attrs) if external_attrs is not None else set()
        self.__rows: list[dict[str, Any]] = []
        self.__projected_attrs: set[str] = self.__primary | self.__attrs
        self.__restriction: Optional[list[PrimaryKey]] = None
        assert self.__primary.isdisjoint(self.__attrs)
        assert self.__external_attrs <= self.__attrs

    def insert(self, rows: Iterable[Mapping[str, Any]]) -> None:
        for row in rows:
            row = dict(row)
            assert set(row) == self.__primary | self.__attrs
            assert {k: v for k, v in row.items() if k in self.__primary} not in self.proj().fetch()
            for attr in self.__external_attrs:
                filepath = Path(row[attr])
                with filepath.open(mode="rb") as file:
                    row[attr] = (filepath.name, file.read())
            self.__rows.append(row)

    def fetch(self, download_path: str = ".") -> list[dict[str, Any]]:
        def project_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
            return [{attr: value for attr, value in row.items() if attr in self.__projected_attrs} for row in rows]

        def convert_external_attrs(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
            converted_rows = [{k: v for k, v in row.items()} for row in rows]
            external_attrs = self.__external_attrs & self.__projected_attrs
            for row in converted_rows:
                for attr in external_attrs:
                    row[attr] = convert_external_attr(*row[attr])
            return converted_rows

        def convert_external_attr(filename: str, data: bytes) -> str:
            filepath = download_path / Path(filename)
            with filepath.open(mode="wb") as file:
                file.write(data)
            return str(filepath)

        return convert_external_attrs(project_rows(self.__rows_in_restriction()))

    def delete(self) -> None:
        def is_confirmed() -> bool:
            answer = None
            while answer not in ("y", "n"):
                answer = input("Really delete? [y/n]: ")
            return answer == "y"

        if is_confirmed():
            self.delete_quick()

    def delete_quick(self) -> None:
        indices = (self.__rows.index(row) for row in self.__rows_in_restriction())
        for index in sorted(indices, reverse=True):
            del self.__rows[index]

    def proj(self, *attributes: str) -> FakeTable:
        attrs = set(attributes)
        assert attrs <= self.__attrs
        table = self.__create_copy()
        table.__projected_attrs = self.__primary | attrs
        return table

    def __and__(self, condition: Union[str, PrimaryKey, Iterable[PrimaryKey]]) -> FakeTable:
        if isinstance(condition, str):
            match = re.compile(r'(^[\w_]+) = "(\w+)"$').match(condition)
            assert match
            attr, value = match.groups()
            rows = (row for row in self.__rows if row[attr] == value)
            condition = [{attr: value for attr, value in row.items() if attr in self.__primary} for row in rows]
        elif isinstance(condition, Mapping):
            condition = [condition]
        else:
            condition = list(condition)
        table = self.__create_copy()
        table.__restriction = condition
        return table

    def children(self, *, as_objects: Literal[True]) -> Sequence[FakeTable]:
        return list(self.__children)

    @property
    def table_name(self) -> str:
        return self.__name

    def __rows_in_restriction(self) -> Iterator[dict[str, Any]]:
        if self.__restriction is not None:
            return (row for row in self.__rows if {k: row[k] for k in self.__primary} in self.__restriction)
        else:
            return iter(self.__rows)

    def __create_copy(self) -> FakeTable:
        table = type(self)(self.__name, primary=self.__primary, attrs=self.__attrs)
        table.__rows = self.__rows
        table.__projected_attrs = self.__projected_attrs
        table.__restriction = self.__restriction
        table.__children = self.__children
        table.__external_attrs = self.__external_attrs
        return table


def test_external_data_handling(tmpdir: Path) -> None:
    filepath = tmpdir / "myfile"
    data = os.urandom(1024)
    with filepath.open(mode="wb") as file:
        file.write(data)
    table = FakeTable("mytable", primary=["a"], attrs=["file"], external_attrs=["file"])
    table.insert([{"a": 0, "file": str(filepath)}])
    os.remove(filepath)
    row = table.fetch(download_path=str(tmpdir))[0]
    with Path(row["file"]).open(mode="rb") as file:
        assert data == file.read()


class DJLinkFacade(AbstractDJLinkFacade):
    def __init__(self, source: Table, outbound: Table, local: Table, *, temp_path: Path = Path(".")) -> None:
        self.source = source
        self.outbound = outbound
        self.local = local
        self.__temp_path = temp_path

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
        rows = (self.outbound & 'is_flagged = "TRUE"').proj().fetch()
        return cast("list[PrimaryKey]", rows)

    def add_to_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        def is_part_table(parent: Table, child: Table) -> bool:
            return child.table_name.startswith(parent.table_name + "__")

        def add_parts_to_local() -> None:
            local_children = {child.table_name: child for child in self.local.children(as_objects=True)}
            for source_child in self.source.children(as_objects=True):
                if not is_part_table(self.source, source_child):
                    continue
                local_children[source_child.table_name].insert((source_child & primary_keys).fetch())

        primary_keys = list(primary_keys)
        self.local.insert((self.source & primary_keys).fetch())
        add_parts_to_local()

    def remove_from_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        (self.local & primary_keys).delete()

    def deprecate(self, primary_keys: Iterable[PrimaryKey]) -> None:
        self.__update_rows(self.outbound, primary_keys, {"process": "NONE", "is_deprecated": "TRUE"})

    def start_pull_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        self.outbound.insert(
            (dict(key, process="PULL", is_flagged="FALSE", is_deprecated="FALSE") for key in primary_keys)
        )

    def finish_pull_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        self.__update_rows(self.outbound, primary_keys, {"process": "NONE"})

    def start_delete_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        self.__update_rows(self.outbound, primary_keys, {"process": "DELETE"})

    def finish_delete_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        self.__update_rows(self.outbound, primary_keys, {"process": "NONE"})

    @staticmethod
    def __update_rows(table: Table, primary_keys: Iterable[PrimaryKey], changes: Mapping[str, Any]) -> None:
        primary_keys = list(primary_keys)
        rows = (table & primary_keys).fetch()
        for row in rows:
            row.update(changes)
        (table & primary_keys).delete_quick()
        table.insert(rows)


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

    def apply(self, updates: Iterable[Update]) -> None:
        def keyfunc(update: Update) -> int:
            assert update.command is not None
            return update.command.value

        transition_updates = (update for update in updates if update.command)
        for command_value, command_updates in groupby(sorted(transition_updates, key=keyfunc), key=keyfunc):
            primary_keys = (self.translator.to_primary_key(update.identifier) for update in updates)
            if Commands(command_value) is Commands.ADD_TO_LOCAL:
                self.facade.add_to_local(primary_keys)
            if Commands(command_value) is Commands.REMOVE_FROM_LOCAL:
                self.facade.remove_from_local(primary_keys)
            if Commands(command_value) is Commands.START_PULL_PROCESS:
                self.facade.start_pull_process(primary_keys)
            if Commands(command_value) is Commands.FINISH_PULL_PROCESS:
                self.facade.finish_pull_process(primary_keys)
            if Commands(command_value) is Commands.DEPRECATE:
                self.facade.deprecate(primary_keys)
            if Commands(command_value) is Commands.START_DELETE_PROCESS:
                self.facade.start_delete_process(primary_keys)
            if Commands(command_value) is Commands.FINISH_DELETE_PROCESS:
                self.facade.finish_delete_process(primary_keys)


class Tables(TypedDict):
    source: FakeTable
    outbound: FakeTable
    local: FakeTable


def create_tables(
    name: str,
    primary: Iterable[str],
    non_primary: Iterable[str],
    *,
    external: Optional[Iterable[str]] = None,
    children: Optional[Mapping[str, Iterable[str]]] = None,
    children_external: Optional[Mapping[str, Iterable[str]]] = None,
) -> Tables:
    def create_child_tables(
        children_non_primary: Mapping[str, Iterable[str]], external_attrs: Mapping[str, Iterable[str]]
    ) -> list[FakeTable]:
        return [
            FakeTable(name, set(primary), set(child_non_primary), external_attrs=external_attrs.get(name, set()))
            for name, child_non_primary in children_non_primary.items()
        ]

    if children is None:
        children = {}
    if children_external is None:
        children_external = {}
    return {
        "source": FakeTable(
            name,
            set(primary),
            set(non_primary),
            children=create_child_tables(children, children_external),
            external_attrs=external,
        ),
        "outbound": FakeTable(name + "_outbound", set(primary), {"process", "is_flagged", "is_deprecated"}),
        "local": FakeTable(
            name,
            set(primary),
            set(non_primary),
            children=create_child_tables(children, children_external),
            external_attrs=external,
        ),
    }


def create_gateway(tables: Tables, *, temp_path: Path = Path(".")) -> DJLinkGateway:
    facade = DJLinkFacade(**tables, temp_path=temp_path)
    translator = IdentificationTranslator()
    return DJLinkGateway(facade, translator)


@dataclass(frozen=True)
class TableState:
    main: list[dict[str, Any]] = field(default_factory=list)
    children: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


@dataclass(frozen=True)
class State:
    source: TableState = field(default_factory=TableState)
    outbound: TableState = field(default_factory=TableState)
    local: TableState = field(default_factory=TableState)

    def __post_init__(self) -> None:
        assert set(self.source.children) == set(self.local.children)


def set_state(tables: Tables, state: State) -> None:
    def set_children_state(table_kind: Union[Literal["source"], Literal["outbound"], Literal["local"]]) -> None:
        for table in tables[table_kind].children(as_objects=True):
            table.insert(getattr(state, table_kind).children.get(table.table_name, []))

    tables["source"].insert(state.source.main)
    tables["outbound"].insert(state.outbound.main)
    tables["local"].insert(state.local.main)
    set_children_state("source")
    set_children_state("outbound")
    set_children_state("local")


def has_state(tables: Tables, expected: State, *, download_path: Path = Path(".")) -> bool:
    def get_children_state(table: Table) -> dict[str, list[dict[str, Any]]]:
        return {
            child.table_name: child.fetch(download_path=str(download_path)) for child in table.children(as_objects=True)
        }

    def get_state() -> State:
        return State(
            source=TableState(
                main=tables["source"].fetch(download_path=str(download_path)),
                children=get_children_state(tables["source"]),
            ),
            outbound=TableState(
                main=tables["outbound"].fetch(download_path=str(download_path)),
                children=get_children_state(tables["outbound"]),
            ),
            local=TableState(
                main=tables["local"].fetch(download_path=str(download_path)),
                children=get_children_state(tables["local"]),
            ),
        )

    return get_state() == expected


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
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState(
                [
                    {"a": 0, "b": 1},
                    {"a": 1, "b": 2},
                    {"a": 2, "b": 3},
                ]
            ),
            outbound=TableState(
                [
                    {"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                    {"a": 1, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                    {"a": 2, "process": "NONE", "is_flagged": "TRUE", "is_deprecated": "FALSE"},
                ]
            ),
            local=TableState(
                [
                    {"a": 2, "b": 3},
                    {"a": 0, "b": 1},
                ]
            ),
        ),
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


def test_add_to_local_command(tmp_path_factory: pytest.TempPathFactory) -> None:
    tables = create_tables(
        "link",
        primary={"a"},
        non_primary={"b", "external"},
        external={"external"},
        children={"link__part1": ["c", "part1_external"], "link__part2": ["d", "part2_external"], "non_part": ["e"]},
        children_external={"link__part1": ["part1_external"], "link__part2": ["part2_external"]},
    )
    gateway = create_gateway(tables)
    base_path = tmp_path_factory.mktemp("set_state")
    main_external_path = base_path / "main_external"
    with main_external_path.open(mode="wb") as file:
        file.write(os.urandom(1024))
    part1_external_path = base_path / "part1_external"
    with part1_external_path.open(mode="wb") as file:
        file.write(os.urandom(1024))
    part2_external_path = base_path / "part2_external"
    with part2_external_path.open(mode="wb") as file:
        file.write(os.urandom(1024))
    set_state(
        tables,
        State(
            source=TableState(
                [{"a": 0, "b": 1, "external": main_external_path}],
                children={
                    "link__part1": [{"a": 0, "c": 1, "part1_external": part1_external_path}],
                    "link__part2": [{"a": 0, "d": 4, "part2_external": part2_external_path}],
                    "non_part": [{"a": 0, "e": 12}],
                },
            ),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState(
                children={
                    "link__part1": [],
                    "link__part2": [],
                    "non_part": [],
                }
            ),
        ),
    )

    gateway.apply(process(gateway.create_link()))

    base_path = tmp_path_factory.mktemp("has_state")
    assert has_state(
        tables,
        State(
            source=TableState(
                [{"a": 0, "b": 1, "external": str(base_path / "main_external")}],
                children={
                    "link__part1": [{"a": 0, "c": 1, "part1_external": str(base_path / "part1_external")}],
                    "link__part2": [{"a": 0, "d": 4, "part2_external": str(base_path / "part2_external")}],
                    "non_part": [{"a": 0, "e": 12}],
                },
            ),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState(
                [{"a": 0, "b": 1, "external": str(base_path / "main_external")}],
                children={
                    "link__part1": [{"a": 0, "c": 1, "part1_external": str(base_path / "part1_external")}],
                    "link__part2": [{"a": 0, "d": 4, "part2_external": str(base_path / "part2_external")}],
                    "non_part": [],
                },
            ),
        ),
        download_path=base_path,
    )


def test_remove_from_local_command() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )

    with as_stdin(StringIO("y")):
        gateway.apply(process(gateway.create_link()))

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )


def test_start_pull_process() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(tables, State(source=TableState([{"a": 0, "b": 1}])))

    gateway.apply(pull(gateway.create_link(), requested={gateway.translator.to_identifier({"a": 0})}))

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )


def test_finish_pull_process() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )

    gateway.apply(process(gateway.create_link()))

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )


def test_start_delete_process() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )

    gateway.apply(delete(gateway.create_link(), requested={gateway.translator.to_identifier({"a": 0})}))

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )


def test_finish_delete_process() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )

    gateway.apply(process(gateway.create_link()))

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )


def test_deprecate_process() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "TRUE", "is_deprecated": "FALSE"}]),
        ),
    )

    gateway.apply(process(gateway.create_link()))

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "TRUE", "is_deprecated": "TRUE"}]),
        ),
    )
