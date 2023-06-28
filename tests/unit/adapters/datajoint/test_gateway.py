from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Mapping
from typing import Any, Optional, Protocol, cast

from dj_link.adapters.datajoint.facade import DJLinkFacade as AbstractDJLinkFacade
from dj_link.adapters.datajoint.facade import DJProcess
from dj_link.adapters.datajoint.identification import IdentificationTranslator
from dj_link.custom_types import PrimaryKey
from dj_link.entities.custom_types import Identifier
from dj_link.entities.link import Link, create_link
from dj_link.entities.state import Components, Processes, Update
from dj_link.use_cases.gateway import LinkGateway


class Table(Protocol):
    def insert1(self, row: Mapping[str, Any]) -> None:
        ...

    def fetch(self) -> list[dict[str, Any]]:
        ...

    def proj(self, *attributes: str) -> Table:
        ...


class FakeTable:
    def __init__(self, primary: Iterable[str], attrs: Optional[Iterable[str]] = None) -> None:
        self.__primary = set(primary)
        self.__attrs = set(attrs) if attrs is not None else set()
        assert self.__primary.isdisjoint(self.__attrs)
        self.__rows: list[dict[str, Any]] = []
        self.__restriction: Optional[set[str]] = None

    def insert1(self, row: Mapping[str, Any]) -> None:
        assert set(row) == self.__primary | self.__attrs
        assert {k: v for k, v in row.items() if k in self.__primary} not in self.proj().fetch()
        self.__rows.append(dict(row))

    def fetch(self) -> list[dict[str, Any]]:
        if self.__restriction is None:
            restriction = self.__primary | self.__attrs
        else:
            restriction = self.__restriction
        return [{k: v for k, v in r.items() if k in restriction} for r in self.__rows]

    def proj(self, *attributes: str) -> FakeTable:
        attrs = set(attributes)
        assert attrs <= self.__attrs
        table = FakeTable(attrs=self.__attrs, primary=self.__primary)
        table.__rows = self.__rows
        table.__restriction = self.__primary | attrs
        return table


class DJLinkFacade(AbstractDJLinkFacade):
    def __init__(self, source: Table, outbound: Table, local: Table) -> None:
        self.source = source
        self.outbound = outbound
        self.local = local

    def get_source_primary_keys(self) -> list[PrimaryKey]:
        return cast("list[PrimaryKey]", self.source.proj().fetch())

    def get_outbound_primary_keys(self) -> list[PrimaryKey]:
        return cast("list[PrimaryKey]", self.outbound.proj().fetch())

    def get_local_primary_keys(self) -> list[PrimaryKey]:
        return cast("list[PrimaryKey]", self.local.proj().fetch())

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
        raise NotImplementedError

    def remove_from_local(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError

    def deprecate(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError

    def start_pull_process(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError

    def finish_pull_process(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError

    def start_delete_process(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError

    def finish_delete_process(self, primary_key: PrimaryKey) -> None:
        raise NotImplementedError


class DJLinkGateway(LinkGateway):
    def __init__(self, facade: AbstractDJLinkFacade, translator: IdentificationTranslator) -> None:
        self.facade = facade
        self.translator = translator

    def create_link(self) -> Link:
        source_identifiers = [self.translator.to_identifier(key) for key in self.facade.get_source_primary_keys()]
        outbound_identifiers = [self.translator.to_identifier(key) for key in self.facade.get_outbound_primary_keys()]
        local_identifiers = [self.translator.to_identifier(key) for key in self.facade.get_local_primary_keys()]
        assignments = {
            Components.SOURCE: source_identifiers,
            Components.OUTBOUND: outbound_identifiers,
            Components.LOCAL: local_identifiers,
        }
        persisted_to_domain_process_map = {"PULL": Processes.PULL, "DELETE": Processes.DELETE}
        domain_processes: dict[Processes, set[Identifier]] = defaultdict(set)
        for persisted_process in self.facade.get_processes():
            if persisted_process.current_process == "NONE":
                continue
            domain_process = persisted_to_domain_process_map[persisted_process.current_process]
            domain_processes[domain_process].add(self.translator.to_identifier(persisted_process.primary_key))
        tainted_identifiers = {self.translator.to_identifier(key) for key in self.facade.get_tainted_primary_keys()}

        return create_link(assignments, processes=domain_processes, tainted_identifiers=tainted_identifiers)

    def apply(self, update: Update) -> None:
        raise NotImplementedError


def test_link_creation() -> None:
    source_table, outbound_table, local_table = (
        FakeTable(["a"], ["b"]),
        FakeTable(["a"], ["process", "is_flagged"]),
        FakeTable(["a"], ["b"]),
    )
    facade = DJLinkFacade(source_table, outbound_table, local_table)
    translator = IdentificationTranslator()
    gateway = DJLinkGateway(facade, translator)

    source_table.insert1({"a": 0, "b": 1})
    outbound_table.insert1({"a": 0, "process": "NONE", "is_flagged": "FALSE"})
    local_table.insert1({"a": 0, "b": 1})

    source_table.insert1({"a": 1, "b": 2})
    outbound_table.insert1({"a": 1, "process": "PULL", "is_flagged": "FALSE"})

    source_table.insert1({"a": 2, "b": 3})
    outbound_table.insert1({"a": 2, "process": "NONE", "is_flagged": "TRUE"})
    local_table.insert1({"a": 2, "b": 3})

    link = gateway.create_link()
    assert link == create_link(
        {
            Components.SOURCE: {
                translator.to_identifier({"a": 0}),
                translator.to_identifier({"a": 1}),
                translator.to_identifier({"a": 2}),
            },
            Components.OUTBOUND: {
                translator.to_identifier({"a": 0}),
                translator.to_identifier({"a": 1}),
                translator.to_identifier({"a": 2}),
            },
            Components.LOCAL: {translator.to_identifier({"a": 0}), translator.to_identifier({"a": 2})},
        },
        processes={Processes.PULL: {translator.to_identifier({"a": 1})}},
        tainted_identifiers={translator.to_identifier({"a": 2})},
    )
