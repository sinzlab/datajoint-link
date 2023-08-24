"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Sequence, cast

import datajoint as dj

from dj_link.adapters.controller import DJController
from dj_link.adapters.custom_types import PrimaryKey


class Mixin:
    """Mixin class for adding functionality to the local DataJoint table."""

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
                (source_table_cls,),
                {
                    "pull": create_pull_method(self.controller),
                    "flagged": property(create_flagged_method(self.outbound_table)),
                },
            )(),
        )

    @property
    def outbound(self) -> dj.Table:
        """Return the outbound table."""
        return self.outbound_table()


def create_pull_method(controller: DJController) -> Callable[[dj.Table], None]:
    """Create pull method used by source table."""

    def pull(self: dj.Table) -> None:
        """Pull idle entities from the source table into the local table."""
        primary_keys = self.proj().fetch(as_dict=True)
        controller.pull(primary_keys)

    return pull


def create_flagged_method(outbound_table: Callable[[], dj.Table]) -> Callable[[dj.Table], Sequence[PrimaryKey]]:
    """Create a method that returns the primary keys of all flagged entities when called."""

    def flagged(self: dj.Table) -> Sequence[PrimaryKey]:
        return (outbound_table() & "is_flagged = 'TRUE'").proj().fetch(as_dict=True)

    return flagged


def create_mixin(
    controller: DJController,
    source_table: Callable[[], dj.Table],
    outbound_table: Callable[[], dj.Table],
) -> type[Mixin]:
    """Create a new subclass of the mixin that is configured to work with a specific link."""
    return type(
        Mixin.__name__,
        (Mixin,),
        {
            "controller": controller,
            "source_table": staticmethod(source_table),
            "outbound_table": staticmethod(outbound_table),
        },
    )
