"""Contains mixins that add functionality to DataJoint tables."""
from __future__ import annotations

from collections.abc import Callable
from typing import Any

import datajoint as dj
from datajoint import AndList
from datajoint.user_tables import UserTable

from dj_link.adapters.datajoint.controller import Controller, DJController

from .factory import TableFactory
from .file import ReusableTemporaryDirectory
from .printer import Printer


class LocalTableMixin:
    """Mixin class for adding additional functionality to the local table class."""

    controller: Controller
    temp_dir: ReusableTemporaryDirectory
    source_table_factory: TableFactory
    printer: Printer
    restriction: AndList

    def pull(self, *restrictions) -> None:
        """Pull entities present in the (restricted) source table into the local table."""
        with self.temp_dir:
            self.controller.pull(AndList(restrictions))
        self.printer()

    def delete(self):
        """Delete entities from the local table."""
        self.controller.delete(self.restriction)
        self.printer()

    def refresh(self):
        """Refresh the repositories."""
        self.controller.refresh()
        self.printer()

    @property
    def source(self) -> type[UserTable]:
        """Return the source table class."""
        return self.source_table_factory()


def create_local_table_mixin_class() -> type[LocalTableMixin]:
    """Create a new subclass of the local table mixin."""
    return type(LocalTableMixin.__name__, (LocalTableMixin,), {})


class Mixin:
    """Mixin class for adding functionality to the local DataJoint table."""

    controller: DJController
    local_table: Callable[[], dj.Table]
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


def create_mixin(
    controller: DJController, source_table: Callable[[], dj.Table], local_table: Callable[[], dj.Table]
) -> type[Mixin]:
    """Create a new subclass of the mixin that is configured to work with a specific link."""
    return type(
        Mixin.__name__, (Mixin,), {"controller": controller, "source_table": source_table, "local_table": local_table}
    )
