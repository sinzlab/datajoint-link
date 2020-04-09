from itertools import product, chain
from contextlib import contextmanager

import datajoint as dj


class Remote:
    def __init__(self, schema):
        self.schema = schema
        self.conn = schema.connection
        self.database = schema.database
        self.host = schema.connection.conn_info["host"]
        self.table = None
        self.outbound = None

    def start_transaction(self):
        self.conn.start_transaction()

    def cancel_transaction(self):
        self.conn.cancel_transaction()

    def commit_transaction(self):
        self.conn.commit_transaction()

    def spawn_missing_classes(self, context=None):
        return self.schema.spawn_missing_classes(context=context)

    @property
    def is_connected(self):
        if self.conn.is_connected:
            return True
        else:
            return False


class Local:
    def __init__(self, schema):
        self.schema = schema
        self.conn = dj.Connection(schema.connection.conn_info["host"], "datajoint", "datajoint")
        self.database = schema.database
        self.host = schema.connection.conn_info["host"]
        self.table = None
        self.inbound = None

    def start_transaction(self):
        self.conn.start_transaction()

    def cancel_transaction(self):
        self.conn.cancel_transaction()

    def commit_transaction(self):
        self.conn.commit_transaction()

    @property
    def is_connected(self):
        if self.conn.is_connected:
            return True
        else:
            return False


class Link:
    def __init__(self, local_schema, remote_schema):
        self._local = Local(local_schema)
        self._remote = Remote(remote_schema)

    def __call__(self, table_cls):
        self.set_up_remote_table(table_cls)
        self.set_up_outbound_table(table_cls)
        self.set_up_inbound_table(table_cls)
        self.set_up_local_table(table_cls)
        return self.local.table

    @property
    def local(self):
        if not self._local.is_connected:
            raise RuntimeError("Missing connection to local host")
        else:
            return self._local

    @property
    def remote(self):
        if not self._remote.is_connected:
            raise RuntimeError("Missing connection to remote host")
        else:
            return self._remote

    def set_up_remote_table(self, table_cls):
        remote_tables = dict()
        self.remote.schema.spawn_missing_classes(context=remote_tables)
        self.remote.table = remote_tables[table_cls.__name__]

    def set_up_outbound_table(self, table_cls):
        class OutboundTable(dj.Lookup):
            remote_table = self.remote.table
            definition = """
            remote_host: varchar(64)
            remote_schema: varchar(64)
            -> self.remote_table
            """

            class Flagged(dj.Part):
                definition = """
                -> master
                """

        OutboundTable.__name__ = table_cls.__name__ + "Outbound"
        outbound_schema = dj.schema("datajoint_outbound__" + self.remote.database, connection=self.remote.conn)
        self.remote.outbound = outbound_schema(OutboundTable)

    def set_up_inbound_table(self, table_cls):
        class InboundTable(dj.Lookup):
            definition = str(self.remote.outbound().heading)

            class Flagged(dj.Part):
                definition = """
                -> master
                """

        InboundTable.__name__ = table_cls.__name__ + "Inbound"
        inbound_schema = dj.schema("datajoint_inbound__" + self.local.database, connection=self.local.conn)
        self.local.inbound = inbound_schema(InboundTable)

    def set_up_local_table(self, table_cls):
        class LocalTable(dj.Lookup):
            link = self
            definition = """
            -> self.link.inbound_table
            """

            @property
            def remote(self):
                return self.link.remote.table()

            @property
            def flagged(self):
                self.link.refresh()
                return self.link.local.inbound().Flagged()

            def pull(self, restriction=None):
                self.link.pull(restriction=restriction)

            def delete(self, verbose=True):
                super().delete(verbose=verbose)
                self.link.refresh()

            def delete_quick(self, get_count=False):
                super().delete_quick(get_count=get_count)
                self.link.refresh()

        LocalTable.__name__ = table_cls.__name__
        heading = self.remote.table().heading
        secondary = (a for a, n in product(str(heading).split("\n"), heading.secondary_attributes) if a.startswith(n))
        LocalTable.definition = "\n".join(chain([LocalTable.definition], secondary))
        self.local.table = self.local.schema(LocalTable)

    def refresh(self):
        with self.transaction():
            self.delete_obsolete_flags()
            self.delete_obsolete_entities()
            self.pull_new_flags()

    def delete_obsolete_flags(self):
        (self.local.inbound().Flagged() - self.local.table()).delete_quick()
        not_obsolete_flags = self.local.inbound().Flagged().fetch()
        (self.remote.outbound().Flagged() - not_obsolete_flags).delete_quick()

    def delete_obsolete_entities(self):
        (self.local.inbound() - self.local.table()).delete_quick()
        not_obsolete_entities = self.local.inbound().fetch()
        (self.remote.outbound() - not_obsolete_entities).delete_quick()

    def pull_new_flags(self):
        outbound_flags = self.remote.outbound().Flagged().fetch()
        self.local.inbound().Flagged().insert(self.local.inbound() & outbound_flags, skip_duplicates=True)

    def pull(self, restriction=None):
        if restriction is None:
            restriction = dj.AndList()
        self.refresh()
        primary_keys = (self.remote.table().proj() & restriction).fetch(as_dict=True)
        entities = (self.remote.table() & restriction).fetch(as_dict=True)
        with self.transaction():
            self.remote.outbound().insert(
                [dict(pk, remote_host=self.local.host, remote_schema=self.local.database) for pk in primary_keys],
                skip_duplicates=True,
            )
            self.local.inbound().insert(
                [dict(pk, remote_host=self.remote.host, remote_schema=self.remote.database) for pk in primary_keys],
                skip_duplicates=True,
            )
            self.local.table().insert(
                [dict(e, remote_host=self.remote.host, remote_schema=self.remote.database) for e in entities],
                skip_duplicates=True,
            )

    @contextmanager
    def transaction(self):
        old_local_table_conn = self.local.table.connection
        try:
            self.remote.start_transaction()
            self.local.start_transaction()
            self.local.table.connection = self.local.conn
            yield
        except Exception:
            self.remote.cancel_transaction()
            self.local.cancel_transaction()
            raise
        else:
            self.remote.commit_transaction()
            self.local.commit_transaction()
        finally:
            self.local.table.connection = old_local_table_conn


link = Link
