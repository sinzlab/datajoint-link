from typing import Type

from datajoint.table import Table
from datajoint.errors import LostConnectionError

from .outbound import OutboundTableFactory


class LocalTableFactory(OutboundTableFactory):
    def __call__(self) -> Table:
        try:
            table_cls = self.spawn_table_cls()
        except KeyError:
            try:
                table_cls = self.create_table_cls()
            except LostConnectionError:
                raise RuntimeError
        return table_cls()

    def spawn_table_cls(self) -> Type[Table]:
        pass

    def create_table_cls(self) -> Type[Table]:
        pass
