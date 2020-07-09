from datajoint.errors import LostConnectionError

from .outbound import OutboundTableFactory


class LocalTableFactory(OutboundTableFactory):
    def __call__(self):
        try:
            return self.spawn_table()
        except KeyError:
            try:
                return self.create_table()
            except LostConnectionError:
                raise RuntimeError

    def spawn_table(self):
        pass

    def create_table(self):
        pass
