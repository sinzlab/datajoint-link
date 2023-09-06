"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Protocol, Sequence, cast

import datajoint as dj

from dj_link.adapters.controller import DJController
from dj_link.adapters.custom_types import PrimaryKey

from . import DJTables


class SourceEndpoint(Protocol):
    """Protocol for the source endpoint."""

    in_transit: TransitMixin


class LocalMixin:
    """Mixin class for the local endpoint."""

    proj: Callable[[], dj.Table]
    _controller: DJController
    _source: Callable[[], SourceEndpoint]

    def delete(self) -> None:
        """Delete pulled entities from the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        self._controller.delete(primary_keys)
        self.source.in_transit.process()

    @property
    def source(self) -> SourceEndpoint:
        """Return the source endpoint."""
        return self._source()


def create_local_mixin(controller: DJController, source: Callable[[], SourceEndpoint]) -> type[LocalMixin]:
    """Create a new subclass of the mixin that is configured to work with a specific link."""
    return type(LocalMixin.__name__, (LocalMixin,), {"_controller": controller, "_source": staticmethod(source)})


class SourceMixin:
    """Mixin class for the source endpoint."""

    proj: Callable[[], dj.Table]
    in_transit: TransitMixin
    _controller: DJController
    _outbound_table: Callable[[], dj.Table]
    __and__: Callable[[SourceMixin, dj.Table], dj.Table]

    def pull(self) -> None:
        """Pull idle entities from the source table into the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        self._controller.pull(primary_keys)
        self.in_transit.process()

    @property
    def flagged(self) -> Sequence[PrimaryKey]:
        """Return the primary keys of all flagged entities."""
        return (self._outbound_table() & "is_flagged = 'TRUE'").proj().fetch(as_dict=True)


def create_source_endpoint_factory(
    controller: DJController, source_table: Callable[[], dj.Table], outbound_table: Callable[[], dj.Table]
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


class TransitEndpoint(Protocol):
    """Protocol for the transit endpoint."""

    def process(self) -> None:
        """Process all transiting entities."""

    def __and__(self, condition: dj.Table) -> TransitEndpoint:
        """Restrict the endpoint with the given condition."""


class TransitMixin:
    """Mixin class used to create the transit endpoint when combined with the source table."""

    proj: Callable[[], dj.Table]
    _controller: DJController

    def process(self) -> None:
        """Process all transiting entities."""
        while primary_keys := self.proj().fetch(as_dict=True):
            self._controller.process(primary_keys)


def create_transit_endpoint(
    controller: DJController, source_table: dj.Table, outbound_table: dj.Table
) -> TransitEndpoint:
    """Create the endpoint responsible for transiting entities."""
    in_transit_cls = cast(
        "type[TransitEndpoint]",
        type(type(source_table).__name__, (TransitMixin, type(source_table)), {"_controller": controller}),
    )
    return in_transit_cls() & (outbound_table & "process != 'NONE'")


def create_local_endpoint(controller: DJController, tables: DJTables) -> dj.Table:
    """Create the local endpoint."""
    return cast(
        dj.Table,
        type(
            type(tables.local()).__name__,
            (
                create_local_mixin(
                    controller, create_source_endpoint_factory(controller, tables.source, tables.outbound)
                ),
                type(tables.local()),
            ),
            {},
        ),
    )
