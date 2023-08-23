"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import cast

import datajoint as dj

from dj_link.adapters.controller import DJController


class Mixin:
    """Mixin class for adding functionality to the local DataJoint table."""

    controller: DJController
    local_table: Callable[[], dj.Table]
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
                {"pull": create_pull_method(self.controller)},
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


def create_mixin(
    controller: DJController,
    source_table: Callable[[], dj.Table],
    outbound_table: Callable[[], dj.Table],
    local_table: Callable[[], dj.Table],
) -> type[Mixin]:
    """Create a new subclass of the mixin that is configured to work with a specific link."""
    return type(
        Mixin.__name__,
        (Mixin,),
        {
            "controller": controller,
            "source_table": staticmethod(source_table),
            "outbound_table": staticmethod(outbound_table),
            "local_table": staticmethod(local_table),
        },
    )
