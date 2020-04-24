from contextlib import contextmanager
from inspect import isclass
import warnings

import datajoint as dj
from datajoint.errors import LostConnectionError


class Host:
    def __init__(self, schema, conn):
        self.schema = schema
        self.conn = conn
        self.database = schema.database
        self.host = schema.connection.conn_info["host"]
        self.main = None
        self.parts = None
        self.gate = None

    def spawn_missing_classes(self, context=None):
        return self.schema.spawn_missing_classes(context=context)

    @property
    def flagged_for_deletion(self):
        return self.gate().FlaggedForDeletion()

    @property
    def transaction(self):
        return self.conn.transaction

    @property
    def is_connected(self):
        return self.conn.is_connected


class Link:
    def __init__(self, local_schema, remote_schema):
        self._local = Host(
            local_schema, dj.Connection(local_schema.connection.conn_info["host"], "datajoint", "datajoint")
        )
        self._remote = Host(remote_schema, remote_schema.connection)

    def __call__(self, table_cls):
        self.set_up_remote_table(table_cls)
        self.set_up_outbound_table(table_cls)
        self.set_up_local_table(table_cls)
        return self.local.main

    @property
    def local(self):
        if not self._local.is_connected:
            raise LostConnectionError("Missing connection to local host")
        else:
            return self._local

    @property
    def remote(self):
        if not self._remote.is_connected:
            raise LostConnectionError("Missing connection to remote host")
        else:
            return self._remote

    def set_up_remote_table(self, table_cls):
        remote_tables = dict()
        self.remote.schema.spawn_missing_classes(context=remote_tables)
        remote_table = remote_tables[table_cls.__name__]
        parts = dict()
        for name in dir(remote_table):
            if name[0].isupper():
                attr = getattr(remote_table, name)
                if isclass(attr) and issubclass(attr, dj.Part):
                    parts[name] = attr
        self.remote.parts = parts
        self.remote.main = remote_table

    def set_up_outbound_table(self, table_cls):
        class OutboundTable(dj.Lookup):
            remote_table = self.remote.main
            definition = """
            remote_host: varchar(64)
            remote_schema: varchar(64)
            -> self.remote_table
            """

            class FlaggedForDeletion(dj.Part):
                definition = """
                -> master
                """

            class ReadyForDeletion(dj.Part):
                definition = """
                -> master
                """

        OutboundTable.__name__ = table_cls.__name__ + "Outbound"
        outbound_schema = dj.schema("datajoint_outbound__" + self.remote.database, connection=self.remote.conn)
        self.remote.gate = outbound_schema(OutboundTable)

    def set_up_local_table(self, table_cls):
        class LocalTable(dj.Lookup):
            link = self

            class FlaggedForDeletion(dj.Part):
                definition = """
                -> master
                """

            @property
            def definition(self):
                return f"""
                remote_host: varchar(64)
                remote_schema: varchar(64)
                {self.link.remote.main().heading}
                """

            @property
            def remote(self):
                return self.link.remote.main()

            @property
            def flagged_for_deletion(self):
                self.link.refresh()
                return self.FlaggedForDeletion()

            def pull(self, *restrictions):
                self.link.pull(restrictions=restrictions)

            def delete(self, verbose=True):
                super().delete(verbose=verbose)
                self.link.refresh()

            def delete_quick(self, get_count=False):
                super().delete_quick(get_count=get_count)
                self.link.refresh()

        local_parts = dict()
        for name, remote_part in self.remote.parts.items():
            local_part = type(name, (dj.Part,), dict(definition="-> master\n" + str(remote_part().heading)))
            local_parts[name] = local_part

        for name, local_part in local_parts.items():
            setattr(LocalTable, name, local_part)

        LocalTable.__name__ = table_cls.__name__
        self.local.parts = local_parts
        self.local.main = self.local.schema(LocalTable)

    def refresh(self):
        try:
            self._refresh()
        except LostConnectionError:
            warnings.warn("Couldn't refresh tables. Check connection to remote host")

    def _refresh(self):
        with self.transaction():
            self.pull_new_flags()
            self.refresh_ready_for_deletion()

    def pull_new_flags(self):
        outbound_flags = self.remote.flagged_for_deletion.fetch(as_dict=True)
        outbound_flags = self.translate(outbound_flags, self.remote)
        self.local.main().FlaggedForDeletion().insert(self.local.main().proj() & outbound_flags, skip_duplicates=True)

    def refresh_ready_for_deletion(self):
        primary_keys = self.local.main().proj().fetch(as_dict=True)
        primary_keys = self.translate(primary_keys, self.local)
        self.remote.gate().ReadyForDeletion().insert(
            self.remote.gate().FlaggedForDeletion() - primary_keys, skip_duplicates=True
        )

    @staticmethod
    def translate(keys, host):
        translated = [{k: v for k, v in x.items() if k not in ("remote_host", "remote_schema")} for x in keys]
        translated = [{"remote_host": host.host, "remote_schema": host.database, **x} for x in translated]
        return translated

    def pull(self, restrictions):
        restrictions = dj.AndList(restrictions)
        self.refresh()
        primary_keys = (self.remote.main().proj() & restrictions).fetch(as_dict=True)
        main_entities = (self.remote.main() & restrictions).fetch(as_dict=True)
        part_entities = {n: (p() & restrictions).fetch(as_dict=True) for n, p in self.remote.parts.items()}
        with self.transaction():
            self.remote.gate().insert(
                [dict(pk, remote_host=self.local.host, remote_schema=self.local.database) for pk in primary_keys],
                skip_duplicates=True,
            )
            self.local.main().insert(
                [dict(e, remote_host=self.remote.host, remote_schema=self.remote.database) for e in main_entities],
                skip_duplicates=True,
            )
            for name, part in self.local.parts.items():
                part().insert(
                    [
                        dict(e, remote_host=self.remote.host, remote_schema=self.remote.database)
                        for e in part_entities[name]
                    ],
                    skip_duplicates=True,
                )

    @contextmanager
    def transaction(self):
        old_local_main_conn = self.local.main.connection
        old_local_parts_conns = {n: p.connection for n, p in self.local.parts.items()}
        try:
            self.local.main.connection = self.local.conn
            for part in self.local.parts.values():
                part.connection = self.local.conn
            with self.local.transaction, self.remote.transaction:
                yield
        finally:
            self.local.main.connection = old_local_main_conn
            for name, part in self.local.parts.items():
                part.connection = old_local_parts_conns[name]


link = Link
