from __future__ import annotations

import os
import re
import sys
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from copy import deepcopy
from dataclasses import dataclass, field
from io import StringIO
from pathlib import Path
from types import TracebackType
from typing import Any, Literal, Optional, TextIO, Type, TypedDict, Union

import pytest

from link.adapters import PrimaryKey
from link.adapters.gateway import DJLinkGateway
from link.adapters.identification import IdentificationTranslator
from link.domain import events
from link.domain.link import create_link
from link.domain.state import Components, Operations, Processes
from link.infrastructure.facade import DJLinkFacade, Table


class FakeConnection:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.__rows = rows
        self.__backup: Optional[list[dict[str, Any]]] = None

    @property
    @contextmanager
    def transaction(self) -> Iterator[FakeConnection]:
        self.__backup = deepcopy(self.__rows)
        try:
            yield self
        except Exception as exception:
            assert self.__backup is not None
            self.__rows.clear()
            self.__rows.extend(self.__backup)
            raise exception
        finally:
            self.__backup = None


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
        self.__connection = FakeConnection(self.__rows)
        self.error_on_insert: Optional[type[Exception]] = None
        assert self.__primary.isdisjoint(self.__attrs)
        assert self.__external_attrs <= self.__attrs

    def insert(self, rows: Iterable[Mapping[str, Any]]) -> None:
        if self.error_on_insert:
            raise self.error_on_insert
        for row in rows:
            row = dict(row)
            assert set(row) == self.__primary | self.__attrs
            assert {k: v for k, v in row.items() if k in self.__primary} not in self.proj().fetch(as_dict=True)
            for attr in self.__external_attrs:
                filepath = Path(row[attr])
                with filepath.open(mode="rb") as file:
                    row[attr] = (filepath.name, file.read())
            self.__rows.append(row)

    def fetch(self, *, as_dict: Literal[True], download_path: str = ".") -> list[dict[str, Any]]:
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

    @property
    def connection(self) -> FakeConnection:
        return self.__connection

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


def initialize(
    name: str, primary: Iterable[str], non_primary: Iterable[str], initial: State
) -> tuple[Tables, DJLinkGateway]:
    tables = create_tables(name, primary, non_primary)
    set_state(tables, initial)
    return tables, create_gateway(tables)


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


def create_gateway(tables: Tables) -> DJLinkGateway:
    def create_table_factory(table: FakeTable) -> Callable[[], FakeTable]:
        def create_table() -> FakeTable:
            return table

        return create_table

    facade = DJLinkFacade(
        source=create_table_factory(tables["source"]),
        outbound=create_table_factory(tables["outbound"]),
        local=create_table_factory(tables["local"]),
    )
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


def has_state(tables: Tables, expected: State) -> bool:
    def get_children_state(table: Table) -> dict[str, list[dict[str, Any]]]:
        return {child.table_name: child.fetch(as_dict=True) for child in table.children(as_objects=True)}

    def get_state() -> State:
        return State(
            source=TableState(
                main=tables["source"].fetch(as_dict=True),
                children=get_children_state(tables["source"]),
            ),
            outbound=TableState(
                main=tables["outbound"].fetch(as_dict=True),
                children=get_children_state(tables["outbound"]),
            ),
            local=TableState(
                main=tables["local"].fetch(as_dict=True),
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


def apply_update(gateway: DJLinkGateway, operation: Operations, requested: Iterable[PrimaryKey]) -> None:
    link = gateway.create_link()
    for entity in link:
        if entity.identifier not in {gateway.translator.to_identifier(key) for key in requested}:
            continue
        entity.apply(operation)
        while entity.events:
            event = entity.events.popleft()
            if not isinstance(event, events.StateChanged):
                continue
            gateway.apply([event])


def test_add_to_local_command() -> None:
    tables = create_tables(
        "link",
        primary={"a"},
        non_primary={"b"},
        children={"link__part1": ["c"], "link__part2": ["d"], "non_part": ["e"]},
    )
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState(
                [{"a": 0, "b": 1}],
                children={
                    "link__part1": [{"a": 0, "c": 1}],
                    "link__part2": [{"a": 0, "d": 4}],
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

    apply_update(gateway, Operations.PROCESS, [{"a": 0}])

    assert has_state(
        tables,
        State(
            source=TableState(
                [{"a": 0, "b": 1}],
                children={
                    "link__part1": [{"a": 0, "c": 1}],
                    "link__part2": [{"a": 0, "d": 4}],
                    "non_part": [{"a": 0, "e": 12}],
                },
            ),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState(
                [{"a": 0, "b": 1}],
                children={
                    "link__part1": [{"a": 0, "c": 1}],
                    "link__part2": [{"a": 0, "d": 4}],
                    "non_part": [],
                },
            ),
        ),
    )


def test_add_to_local_command_with_error() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"}, children={"link__part": {"c"}})
    gateway = create_gateway(tables)
    initial_state = State(
        source=TableState([{"a": 0, "b": 1}], children={"link__part": [{"a": 0, "c": 1}]}),
        outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        local=TableState(children={"link__part": []}),
    )
    set_state(tables, initial_state)

    tables["local"].children(as_objects=True)[0].error_on_insert = RuntimeError
    try:
        apply_update(gateway, Operations.PROCESS, [{"a": 0}])
    except RuntimeError:
        pass

    assert has_state(tables, initial_state)


def test_add_to_local_command_with_external_file(tmpdir: Path) -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"external"}, external={"external"})
    gateway = create_gateway(tables)
    insert_filepath = tmpdir / "file"
    data = os.urandom(1024)
    with insert_filepath.open(mode="wb") as file:
        file.write(data)
    tables["source"].insert([{"a": 0, "external": insert_filepath}])
    os.remove(insert_filepath)
    tables["outbound"].insert([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}])
    apply_update(gateway, Operations.PROCESS, [{"a": 0}])
    fetch_filepath = Path(tables["local"].fetch(as_dict=True, download_path=str(tmpdir))[0]["external"])
    with fetch_filepath.open(mode="rb") as file:
        assert file.read() == data


def test_remove_from_local_command() -> None:
    tables, gateway = initialize(
        "link",
        primary={"a"},
        non_primary={"b"},
        initial=State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )

    with as_stdin(StringIO("y")):
        apply_update(gateway, Operations.PROCESS, [{"a": 0}])

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )


def test_start_pull_process() -> None:
    tables, gateway = initialize(
        "link", primary={"a"}, non_primary={"b"}, initial=State(source=TableState([{"a": 0, "b": 1}]))
    )

    apply_update(gateway, Operations.START_PULL, [{"a": 0}])

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )


class TestFinishPullProcessCommand:
    @staticmethod
    @pytest.fixture()
    def initial_state() -> State:
        return State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        )

    @staticmethod
    def test_state_after_command(initial_state: State) -> None:
        tables, gateway = initialize("link", primary={"a"}, non_primary={"b"}, initial=initial_state)

        apply_update(gateway, Operations.PROCESS, [{"a": 0}])

        assert has_state(
            tables,
            State(
                source=TableState([{"a": 0, "b": 1}]),
                outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
                local=TableState([{"a": 0, "b": 1}]),
            ),
        )

    @staticmethod
    def test_rollback_on_error(initial_state: State) -> None:
        tables, gateway = initialize("link", primary={"a"}, non_primary={"b"}, initial=initial_state)

        tables["outbound"].error_on_insert = RuntimeError
        try:
            apply_update(gateway, Operations.PROCESS, [{"a": 0}])
        except RuntimeError:
            pass

        assert has_state(tables, initial_state)


class TestStartDeleteProcessCommand:
    @staticmethod
    @pytest.fixture()
    def initial_state() -> State:
        return State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
            local=TableState([{"a": 0, "b": 1}]),
        )

    @staticmethod
    def test_state_after_command(initial_state: State) -> None:
        tables, gateway = initialize("link", primary={"a"}, non_primary={"b"}, initial=initial_state)

        apply_update(gateway, Operations.START_DELETE, [{"a": 0}])

        assert has_state(
            tables,
            State(
                source=TableState([{"a": 0, "b": 1}]),
                outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
                local=TableState([{"a": 0, "b": 1}]),
            ),
        )

    @staticmethod
    def test_rollback_on_error(initial_state: State) -> None:
        tables, gateway = initialize("link", primary={"a"}, non_primary={"b"}, initial=initial_state)

        tables["outbound"].error_on_insert = RuntimeError
        try:
            apply_update(gateway, Operations.START_DELETE, [{"a": 0}])
        except RuntimeError:
            pass

        assert has_state(tables, initial_state)


def test_finish_delete_process_command() -> None:
    tables, gateway = initialize(
        "link",
        primary={"a"},
        non_primary={"b"},
        initial=State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"}]),
        ),
    )

    apply_update(gateway, Operations.PROCESS, [{"a": 0}])

    assert has_state(tables, State(source=TableState([{"a": 0, "b": 1}])))


class TestDeprecateProcessCommand:
    @staticmethod
    @pytest.fixture()
    def initial_state() -> State:
        return State(
            source=TableState([{"a": 0, "b": 1}]),
            outbound=TableState([{"a": 0, "process": "DELETE", "is_flagged": "TRUE", "is_deprecated": "FALSE"}]),
        )

    @staticmethod
    def test_state_after_command(initial_state: State) -> None:
        tables, gateway = initialize("link", primary={"a"}, non_primary={"b"}, initial=initial_state)

        apply_update(gateway, Operations.PROCESS, [{"a": 0}])

        assert has_state(
            tables,
            State(
                source=TableState([{"a": 0, "b": 1}]),
                outbound=TableState([{"a": 0, "process": "NONE", "is_flagged": "TRUE", "is_deprecated": "TRUE"}]),
            ),
        )

    @staticmethod
    def test_rollback_on_error(initial_state: State) -> None:
        tables, gateway = initialize("link", primary={"a"}, non_primary={"b"}, initial=initial_state)

        tables["outbound"].error_on_insert = RuntimeError
        try:
            apply_update(gateway, Operations.PROCESS, [{"a": 0}])
        except RuntimeError:
            pass

        assert has_state(tables, initial_state)


def test_applying_multiple_commands() -> None:
    tables = create_tables("link", primary={"a"}, non_primary={"b"})
    gateway = create_gateway(tables)
    set_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}, {"a": 1, "b": 2}]),
            outbound=TableState(
                [
                    {"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                    {"a": 1, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                ]
            ),
            local=TableState([{"a": 1, "b": 2}]),
        ),
    )

    with as_stdin(StringIO("y")):
        apply_update(gateway, Operations.PROCESS, [{"a": 0}, {"a": 1}])

    assert has_state(
        tables,
        State(
            source=TableState([{"a": 0, "b": 1}, {"a": 1, "b": 2}]),
            outbound=TableState(
                [
                    {"a": 0, "process": "PULL", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                    {"a": 1, "process": "DELETE", "is_flagged": "FALSE", "is_deprecated": "FALSE"},
                ]
            ),
            local=TableState([{"a": 0, "b": 1}]),
        ),
    )
