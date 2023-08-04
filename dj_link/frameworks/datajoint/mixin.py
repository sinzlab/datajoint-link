"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import datajoint as dj
from datajoint import AndList

from dj_link.adapters.datajoint.controller import DJController


class Mixin:
    """Mixin class for adding functionality to the local DataJoint table."""

    controller: DJController
    local_table: Callable[[], dj.Table]
    outbound_table: Callable[[], dj.Table]
    source_table: Callable[[], dj.Table]
    restriction: Any

    @classmethod
    def pull(cls, *restrictions: Any) -> None:
        """Pull idle entities from the source table into the local table."""
        primary_keys = (cls.source_table().proj() & AndList(restrictions)).fetch(as_dict=True)
        cls.controller.pull(primary_keys)

    @classmethod
    def delete(cls) -> None:
        """Delete pulled entities from the local table."""
        primary_keys = (cls.local_table().proj() & cls.restriction).fetch(as_dict=True)
        cls.controller.delete(primary_keys)

    @property
    def source(self) -> dj.Table:
        """Return the source table."""
        return self.source_table()

    @property
    def outbound(self) -> dj.Table:
        """Return the outbound table."""
        return self.outbound_table()


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
