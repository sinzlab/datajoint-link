"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Sequence, cast

import datajoint as dj

from dj_link.adapters.controller import DJController
from dj_link.adapters.custom_types import PrimaryKey


class LocalMixin:
    """Mixin class for local table."""

    controller: DJController
    outbound_table: Callable[[], dj.Table]
    source_table: Callable[[], dj.Table]
    proj: Callable[[], dj.Table]

    def delete(self) -> None:
        """Delete pulled entities from the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        self.controller.delete(primary_keys)

    @property
    def source(self) -> dj.Table:
        """Return the source table."""
        source_table_cls = type(self.source_table())
        return cast(
            dj.Table,
            type(
                source_table_cls.__name__,
                (SourceMixin, source_table_cls),
                {"controller": self.controller, "outbound_table": staticmethod(self.outbound_table)},
            )(),
        )


class SourceMixin:
    """Mixin class for the source table."""

    proj: Callable[[], dj.Table]
    controller: DJController
    outbound_table: Callable[[], dj.Table]

    def pull(self) -> None:
        """Pull idle entities from the source table into the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        self.controller.pull(primary_keys)

    @property
    def flagged(self) -> Sequence[PrimaryKey]:
        """Return the primary keys of all flagged entities."""
        return (self.outbound_table() & "is_flagged = 'TRUE'").proj().fetch(as_dict=True)


def create_mixin(
    controller: DJController,
    source_table: Callable[[], dj.Table],
    outbound_table: Callable[[], dj.Table],
) -> type[LocalMixin]:
    """Create a new subclass of the mixin that is configured to work with a specific link."""
    return type(
        LocalMixin.__name__,
        (LocalMixin,),
        {
            "controller": controller,
            "source_table": staticmethod(source_table),
            "outbound_table": staticmethod(outbound_table),
        },
    )
