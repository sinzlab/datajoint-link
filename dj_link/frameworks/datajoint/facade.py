"""Contains the DataJoint table facade."""
from __future__ import annotations

from collections.abc import Callable
from tempfile import TemporaryDirectory
from typing import (
    Any,
    ContextManager,
    Iterable,
    Literal,
    Mapping,
    Protocol,
    Sequence,
    Union,
    cast,
)

from dj_link.adapters.datajoint import PrimaryKey
from dj_link.adapters.datajoint.facade import DJAssignments, DJProcess
from dj_link.adapters.datajoint.facade import DJLinkFacade as AbstractDJLinkFacade


class Connection(Protocol):  # pylint: disable=too-few-public-methods
    """DataJoint connection protocol."""

    @property
    def transaction(self) -> ContextManager[Connection]:
        """Context manager for transactions."""


class Table(Protocol):
    """DataJoint table protocol."""

    def insert(self, rows: Iterable[Mapping[str, Any]]) -> None:
        """Insert the given rows into the table."""

    def fetch(self, *, as_dict: Literal[True], download_path: str = ...) -> list[dict[str, Any]]:
        """Fetch rows from the table."""

    def delete(self) -> None:
        """Delete rows from the table."""

    def delete_quick(self) -> None:
        """Delete rows from the table without asking for confirmation."""

    def proj(self, *attributes: str) -> Table:
        """Project the table to the given set of attributes."""

    def __and__(self, condition: Union[str, PrimaryKey, Iterable[PrimaryKey]]) -> Table:
        """Restrict the rows in the table to the ones matching the given condition."""

    def children(self, *, as_objects: Literal[True]) -> Sequence[Table]:
        """Return the children of this table."""

    @property
    def table_name(self) -> str:
        """The table's name (without schema name)."""

    @property
    def connection(self) -> Connection:
        """The table's connection object."""


class DJLinkFacade(AbstractDJLinkFacade):
    """Facade around DataJoint operations needed to interact with stored links."""

    def __init__(self, source: Callable[[], Table], outbound: Callable[[], Table], local: Callable[[], Table]) -> None:
        """Initialize the facade."""
        self.source = source
        self.outbound = outbound
        self.local = local

    def get_assignments(self) -> DJAssignments:
        """Get the assignments of primary keys to tables."""
        return DJAssignments(
            cast("list[PrimaryKey]", self.source().proj().fetch(as_dict=True)),
            cast("list[PrimaryKey]", self.outbound().proj().fetch(as_dict=True)),
            cast("list[PrimaryKey]", self.local().proj().fetch(as_dict=True)),
        )

    def get_processes(self) -> list[DJProcess]:
        """Get the current process (if any) from each entity in the outbound table."""
        rows = self.outbound().proj("process").fetch(as_dict=True)
        processes: list[DJProcess] = []
        for row in rows:
            process = row.pop("process")
            processes.append(DJProcess(row, process))
        return processes

    def get_tainted_primary_keys(self) -> list[PrimaryKey]:
        """Get the flagged (i.e. tainted) primary keys from the outbound table."""
        rows = (self.outbound() & 'is_flagged = "TRUE"').proj().fetch(as_dict=True)
        return cast("list[PrimaryKey]", rows)

    def add_to_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Add the entities corresponding to the given primary keys to the local table."""

        def is_part_table(parent: Table, child: Table) -> bool:
            return child.table_name.startswith(parent.table_name + "__")

        def add_parts_to_local(download_path: str) -> None:
            local_children = {child.table_name: child for child in self.local().children(as_objects=True)}
            for source_child in self.source().children(as_objects=True):
                if not is_part_table(self.source(), source_child):
                    continue
                local_children[source_child.table_name].insert(
                    (source_child & primary_keys).fetch(as_dict=True, download_path=download_path)
                )

        primary_keys = list(primary_keys)
        with self.local().connection.transaction, TemporaryDirectory() as download_path:
            self.local().insert((self.source() & primary_keys).fetch(as_dict=True, download_path=download_path))
            add_parts_to_local(download_path)

    def remove_from_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Remove the entities corresponding to the given primary keys from the local table."""
        (self.local() & primary_keys).delete()

    def deprecate(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Deprecate the entities corresponding to the given primary keys by updating rows in the outbound table."""
        self.__update_rows(self.outbound(), primary_keys, {"process": "NONE", "is_deprecated": "TRUE"})

    def start_pull_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Start the pull process of the entities corresponding to the given primary keys."""
        self.outbound().insert(
            (dict(key, process="PULL", is_flagged="FALSE", is_deprecated="FALSE") for key in primary_keys)
        )

    def finish_pull_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Finish the pull process of the entities corresponding to the given primary keys."""
        self.__update_rows(self.outbound(), primary_keys, {"process": "NONE"})

    def start_delete_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Start the delete process of the entities corresponding to the given primary keys."""
        self.__update_rows(self.outbound(), primary_keys, {"process": "DELETE"})

    def finish_delete_process(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Finish the delete process of the entities corresponding to the given primary keys."""
        (self.outbound() & primary_keys).delete_quick()

    @staticmethod
    def __update_rows(table: Table, primary_keys: Iterable[PrimaryKey], changes: Mapping[str, Any]) -> None:
        with table.connection.transaction:
            primary_keys = list(primary_keys)
            rows = (table & primary_keys).fetch(as_dict=True)
            for row in rows:
                row.update(changes)
            (table & primary_keys).delete_quick()
            table.insert(rows)
