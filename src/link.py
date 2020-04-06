from contextlib import contextmanager

from datajoint import Lookup, Part
from datajoint.schemas import Schema
from datajoint.connection import Connection, conn


class LinkedSchema(Schema):
    def __init__(self, host, schema_name, user="datajoint", password="datajoint"):
        connection = Connection(host, user, password)
        super().__init__(schema_name, connection=connection)


class Link:
    def __init__(self, source_schema, linked_schema):
        self.source_schema = source_schema
        self.linked_schema = linked_schema
        self.source_conn = Connection(source_schema.connection.conn_info["host"], "datajoint", "datajoint")
        self.link_conn = linked_schema.connection
        self.outbound_schema = Schema("datajoint_outbound__" + self.source_schema.database, connection=self.source_conn)
        self.inbound_schema = Schema("datajoint_inbound__" + self.linked_schema.database, connection=self.link_conn)
        self.source_table = None
        self._outbound_table = None
        self._inbound_table = None
        self._linked_table = None

    def __call__(self, table_cls):
        self.create_source_table(table_cls)
        self.create_outbound_table(table_cls)
        self.create_inbound_table(table_cls)
        self.create_linked_table(table_cls)
        return self.source_table

    def create_source_table(self, table_cls):
        class SourceTable(table_cls):
            link = self

            def sync(self):
                self.link.sync(self.restriction)

            def delete(self):
                self.link.refresh()
                super().delete()

            def flag(self):
                self.link.flag(self.restriction)

        SourceTable.__name__ = table_cls.__name__
        self.source_table = self.source_schema(SourceTable)

    @property
    def outbound_table(self):
        if not self._outbound_table().is_declared:
            self.outbound_schema(self._outbound_table)
        return self._outbound_table

    def create_outbound_table(self, table_cls):
        class OutboundTable(Lookup):
            source_table = self.source_table

            definition = """
            link_host: varchar(64)
            link_schema: varchar(64)
            -> self.source_table
            """

        OutboundTable.__name__ = table_cls.__name__ + "Outbound"
        self._outbound_table = self.outbound_schema(OutboundTable)

    @property
    def inbound_table(self):
        if not self._inbound_table().is_declared:
            self.inbound_schema(self._inbound_table)
        return self._inbound_table

    def create_inbound_table(self, table_cls):
        class InboundTable(Lookup):
            definition = f"""
            source_host: varchar(64)
            source_schema: varchar(64)
            {str(self.source_table().proj().heading)}
            """

            class Flagged(Part):
                definition = """
                -> master
                """

        InboundTable.__name__ = table_cls.__name__ + "Inbound"
        self._inbound_table = self.inbound_schema(InboundTable)

    @property
    def linked_table(self):
        if not self._linked_table().is_declared:
            self.linked_schema(self._linked_table)
        return self._linked_table

    def create_linked_table(self, table_cls):
        class LinkedTable(table_cls):
            inbound_table = self.inbound_table

            definition = f"""
            -> self.inbound_table
            ---
            {str(self.source_table().heading).split('---')[-1]}
            """

        LinkedTable.__name__ = table_cls.__name__
        with self.connection(self.link_conn):
            self._linked_table = self.linked_schema(LinkedTable)

    def sync(self, restriction):
        self.refresh()
        link_host = self.linked_schema.connection.conn_info["host"]
        link_schema_name = self.linked_schema.database
        source_host = self.source_schema.connection.conn_info["host"]
        source_schema_name = self.source_schema.database
        with self.connection(self.source_conn):
            query = self.source_table() & restriction
            primary_keys = (query.proj() - self.outbound_table()).fetch(as_dict=True)
            entities = (query - self.outbound_table()).fetch(as_dict=True)
        try:
            with self.connection(self.source_conn) as source_conn:
                source_conn.start_transaction()
                self.outbound_table().insert(
                    [dict(link_host=link_host, link_schema=link_schema_name, **pk) for pk in primary_keys]
                )
            with self.connection(self.link_conn) as link_conn:
                link_conn.start_transaction()
                self.inbound_table().insert(
                    [dict(source_host=source_host, source_schema=source_schema_name, **pk) for pk in primary_keys]
                )
                self.linked_table().insert(
                    [dict(source_host=source_host, source_schema=source_schema_name, **e) for e in entities]
                )
        except Exception:
            source_conn.cancel_transaction()
            link_conn.cancel_transaction()
            raise
        finally:
            source_conn.commit_transaction()
            link_conn.commit_transaction()

    def refresh(self):
        with self.connection(self.link_conn):
            (self.inbound_table().Flagged() - self.linked_table()).delete_quick()
            (self.inbound_table() - self.linked_table()).delete_quick()
            primary_keys = self.inbound_table().proj().fetch()
        with self.connection(self.source_conn):
            (self.outbound_table() - primary_keys).delete_quick()
        if not len(self.outbound_table()):
            self.outbound_table().drop_quick()

    def flag(self, restriction):
        primary_keys = (self.outbound_table() & restriction).fetch(as_dict=True)
        self.inbound_table().Flagged().insert(self.inbound_table() & primary_keys, skip_duplicates=True)

    @contextmanager
    def connection(self, connection):
        old_table_conn = self.source_table.connection
        if hasattr(conn, "connection"):
            old_conn = conn.connection
        else:
            old_conn = None
        conn.connection = connection
        if connection is self.source_conn:
            self.source_table.connection = connection
        try:
            yield connection
        finally:
            if old_conn:
                conn.connection = old_conn
            else:
                delattr(conn, "connection")
            self.source_table.connection = old_table_conn
