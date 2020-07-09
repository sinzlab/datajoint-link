from datajoint.errors import LostConnectionError

from .outbound import OutboundTableFactory


class LocalTableFactory(OutboundTableFactory):
    def __call__(self):
        try:
            table_cls = self.spawn_table_cls()
        except KeyError:
            try:
                table_cls = self.create_table_cls()
            except LostConnectionError:
                raise RuntimeError
        return table_cls()

    def spawn_table_cls(self):
        pass

    def create_table_cls(self):
        pass
