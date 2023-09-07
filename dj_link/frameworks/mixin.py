"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, Sequence, TypeVar, cast

from dj_link.adapters.controller import DJController
from dj_link.adapters.custom_types import PrimaryKey

from . import DJTables


class Table(Protocol):
    """DataJoint table protocol."""

    def fetch(self, as_dict: bool | None = ...) -> list[PrimaryKey]:
        """Fetch entities from the table."""

    def proj(self) -> Table:
        """Project the table onto its primary attributes."""

    def __and__(self: _T, condition: str | Table) -> _T:
        """Restrict the table according to the given condition."""


_T = TypeVar("_T", bound=Table)


class LocalEndpoint(Table, Protocol):
    """Protocol for the local endpoint."""

    _controller: DJController
    _source: Callable[[], SourceEndpoint]

    @property
    def source(self) -> SourceEndpoint:
        """Return the source endpoint."""


class SourceEndpoint(Table, Protocol):
    """Protocol for the source endpoint."""

    _controller: DJController
    _outbound_table: Callable[[], Table]

    in_transit: TransitEndpoint


class TransitEndpoint(Table, Protocol):
    """Protocol for the transit endpoint."""

    _controller: DJController

    def process(self) -> None:
        """Process all transiting entities."""


class LocalMixin:
    """Mixin class for the local endpoint."""

    def delete(self: LocalEndpoint) -> None:
        """Delete pulled entities from the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        self._controller.delete(primary_keys)
        self.source.in_transit.process()

    @property
    def source(self: LocalEndpoint) -> SourceEndpoint:
        """Return the source endpoint."""
        return self._source()


def create_local_mixin(controller: DJController, source: Callable[[], SourceEndpoint]) -> type[LocalMixin]:
    """Create a new subclass of the mixin that is configured to work with a specific link."""
    return type(LocalMixin.__name__, (LocalMixin,), {"_controller": controller, "_source": staticmethod(source)})


class SourceMixin:
    """Mixin class for the source endpoint."""

    def pull(self: SourceEndpoint) -> None:
        """Pull idle entities from the source table into the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        self._controller.pull(primary_keys)
        self.in_transit.process()

    @property
    def flagged(self: SourceEndpoint) -> Sequence[PrimaryKey]:
        """Return the primary keys of all flagged entities."""
        return (self._outbound_table() & "is_flagged = 'TRUE'").proj().fetch(as_dict=True)


def create_source_endpoint_factory(
    controller: DJController, source_table: Callable[[], Table], outbound_table: Callable[[], Table]
) -> Callable[[], SourceEndpoint]:
    """Create a callable that returns the source endpoint when called."""

    def create_source_endpoint() -> SourceEndpoint:
        source_table_cls = type(source_table())
        return cast(
            SourceEndpoint,
            type(
                source_table_cls.__name__,
                (SourceMixin, source_table_cls),
                {
                    "_controller": controller,
                    "_outbound_table": staticmethod(outbound_table),
                    "in_transit": create_transit_endpoint(controller, source_table(), outbound_table()),
                },
            )(),
        )

    return create_source_endpoint


class TransitMixin:
    """Mixin class used to create the transit endpoint when combined with the source table."""

    def process(self: TransitEndpoint) -> None:
        """Process all transiting entities."""
        while primary_keys := self.proj().fetch(as_dict=True):
            self._controller.process(primary_keys)


def create_transit_endpoint(controller: DJController, source_table: Table, outbound_table: Table) -> TransitEndpoint:
    """Create the endpoint responsible for transiting entities."""
    in_transit_cls = cast(
        "type[TransitEndpoint]",
        type(type(source_table).__name__, (TransitMixin, type(source_table)), {"_controller": controller}),
    )
    return in_transit_cls() & (outbound_table & "process != 'NONE'")


def create_local_endpoint(controller: DJController, tables: DJTables) -> LocalEndpoint:
    """Create the local endpoint."""
    return cast(
        LocalEndpoint,
        type(
            type(tables.local()).__name__,
            (
                create_local_mixin(
                    controller,
                    create_source_endpoint_factory(
                        controller, cast(Callable[[], Table], tables.source), cast(Callable[[], Table], tables.outbound)
                    ),
                ),
                type(tables.local()),
            ),
            {},
        ),
    )
