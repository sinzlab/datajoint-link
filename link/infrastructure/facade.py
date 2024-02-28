"""Contains the DataJoint table facade."""
from __future__ import annotations

from collections.abc import Callable
from tempfile import TemporaryDirectory
from typing import Any, ContextManager, Iterable, Literal, Mapping, Protocol, Sequence, Union

from link.adapters import PrimaryKey
from link.adapters.facade import DJAssignment, DJCondition, DJProcess, ProcessType
from link.adapters.facade import DJLinkFacade as AbstractDJLinkFacade


class Connection(Protocol):
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

    def fetch1(self, attrs: str) -> Any:
        """Fetch a single row from the table."""

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

    def __contains__(self, primary_key: PrimaryKey) -> bool:
        """Check if the table contains a row with the given primary key."""

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

    def get_assignment(self, primary_key: PrimaryKey) -> DJAssignment:
        """Get the assignment of the entity with the given primary key."""
        return DJAssignment(
            primary_key, primary_key in self.source(), primary_key in self.outbound(), primary_key in self.local()
        )

    def get_condition(self, primary_key: PrimaryKey) -> DJCondition:
        """Get the condition of the entity with the given primary key."""
        if primary_key not in self.outbound():
            is_flagged = False
        else:
            is_flagged = (self.outbound() & primary_key).fetch1("is_flagged") == "TRUE"
        return DJCondition(primary_key, is_flagged)

    def get_process(self, primary_key: PrimaryKey) -> DJProcess:
        """Get the process of the entity with the given primary key."""
        process: ProcessType
        if primary_key not in self.outbound():
            process = "NONE"
        else:
            process = (self.outbound() & primary_key).fetch1("process")
        return DJProcess(primary_key, process)

    def add_to_local(self, primary_keys: Iterable[PrimaryKey]) -> None:
        """Add the entities corresponding to the given primary keys to the local table."""

        def is_part_table(parent: Table, child: Table) -> bool:
            return child.table_name.startswith(parent.table_name + "__")

        def remove_parent_prefix_from_part_name(parent: Table, part: Table) -> str:
            assert is_part_table(parent, part)
            return part.table_name[len(parent.table_name) :]

        def get_parts(parent: Table) -> dict[str, Table]:
            parts = (child for child in parent.children(as_objects=True) if is_part_table(parent, child))
            return {remove_parent_prefix_from_part_name(parent, part): part for part in parts}

        def add_parts_to_local(download_path: str) -> None:
            local_parts = get_parts(self.local())
            for source_name, source_part in get_parts(self.source()).items():
                local_parts[source_name].insert(
                    (source_part & primary_keys).fetch(as_dict=True, download_path=download_path)
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
