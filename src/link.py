from contextlib import contextmanager
from inspect import isclass
from tempfile import TemporaryDirectory
import warnings
import os

import datajoint as dj
from datajoint.connection import Connection
from datajoint.schemas import Schema
from datajoint.errors import LostConnectionError
from pymysql.err import OperationalError


class ConnectionProxy:
    def __init__(self, host, user, password, port=None, init_fun=None, use_tls=None):
        self.host = host
        self.user = user
        self.password = password
        self.port = port
        self.init_fun = init_fun
        self.use_tls = use_tls

        self.is_initialized = False
        self._connection = None

    @property
    def connection(self):
        if not self.is_initialized:
            raise RuntimeError("Not initialized!")
        return self._connection

    def initialize(self):
        if not self.is_initialized:
            self._initialize()

    def _initialize(self):
        self._connection = Connection(
            self.host, self.user, self.password, port=self.port, init_fun=self.init_fun, use_tls=self.use_tls
        )
        self.is_initialized = True

    def query(self, query, args=(), *, as_dict=False, suppress_warnings=True, reconnect=None):
        return self.connection.query(
            query, args=args, as_dict=as_dict, suppress_warnings=suppress_warnings, reconnect=reconnect
        )

    def get_user(self):
        return self.connection.get_user()

    def register(self, schema):
        self.connection.register(schema=schema)

    @property
    def is_connected(self):
        return self.connection.is_connected

    @property
    def transaction(self):
        return self.connection.transaction

    @property
    def conn_info(self):
        return self.connection.conn_info

    @property
    def in_transaction(self):
        return self.connection.in_transaction

    @property
    def dependencies(self):
        return self.connection.dependencies

    @property
    def schemas(self):
        return self.connection.schemas

    def start_transaction(self):
        self.connection.start_transaction()

    def cancel_transaction(self):
        self.connection.cancel_transaction()

    def commit_transaction(self):
        self.connection.commit_transaction()


class SchemaProxy:
    def __init__(
        self, schema_name, context=None, *, connection=None, create_schema=True, create_tables=True, host=None
    ):
        if host is not None and connection is None:
            connection = ConnectionProxy(host, os.environ["INTERNAL_DJ_USER"], os.environ["INTERNAL_DJ_PASS"])

        self.database = schema_name
        self.context = context
        self.connection = connection
        self.create_schema = create_schema
        self.create_tables = create_tables

        self.is_initialized = False
        self._schema = None

    @property
    def schema(self):
        self.initialize()
        return self._schema

    def initialize(self):
        if not self.is_initialized:
            self._initialize()

    def _initialize(self):
        if self.connection is not None:
            self.connection.initialize()
        schema = Schema(
            self.database,
            context=self.context,
            connection=self.connection,
            create_schema=self.create_schema,
            create_tables=self.create_tables,
        )
        self.connection = schema.connection
        self._schema = schema
        self.is_initialized = True

    @property
    def log(self):
        return self.schema.log

    @property
    def jobs(self):
        return self.schema.jobs

    def spawn_missing_classes(self, context=None):
        self.schema.spawn_missing_classes(context=context)

    def drop(self, force=False):
        self.schema.drop(force=force)

    def __call__(self, cls, *, context=None):
        return self.schema(cls, context=None)

    def __repr__(self):
        return f"Schema `{self.database}`"


class Host:
    def __init__(self, schema):
        self.schema = schema
        self.database = schema.database
        self.main = None
        self.parts = None
        self.gate = None

    def spawn_missing_classes(self, context=None):
        return self.schema.spawn_missing_classes(context=context)

    def initialize(self):
        self.schema.initialize()

    @property
    def conn(self):
        return self.schema.connection

    @property
    def host(self):
        return self.schema.connection.conn_info["host"]

    @property
    def flagged_for_deletion(self):
        return self.gate().FlaggedForDeletion()

    @property
    def transaction(self):
        return self.conn.transaction

    @property
    def is_connected(self):
        if not self.is_initialized:
            return False
        else:
            return self.conn.is_connected

    @property
    def is_initialized(self):
        return self.schema.is_initialized


class Link:
    def __init__(self, local_schema, remote_schema, stores=None):
        self.remote_schema = remote_schema
        self.stores = stores if stores else dict()
        self.table_cls = None
        self._local = Host(local_schema)
        self._remote = Host(remote_schema)

    def __call__(self, table_cls):
        self.table_cls = table_cls
        return self.local.main

    @property
    def local(self):
        if not self._local.is_initialized:
            try:
                self._local.initialize()
            except OperationalError:
                raise LostConnectionError("Missing connection to local host")
            else:
                self._initialize_local()
        elif not self._local.is_connected:
            raise LostConnectionError("Missing connection to local host")
        return self._local

    @property
    def remote(self):
        if not self._remote.is_initialized:
            try:
                self._remote.initialize()
            except OperationalError:
                raise LostConnectionError("Missing connection to remote host")
            else:
                self._initialize_remote()
        elif not self._remote.is_connected:
            raise LostConnectionError("Missing connection to remote host")
        return self._remote

    def _initialize_remote(self):
        self._set_up_remote_table()
        self._set_up_outbound_table()

    def _set_up_remote_table(self):
        remote_tables = dict()
        self.remote.schema.spawn_missing_classes(context=remote_tables)
        remote_table = remote_tables[self.table_cls.__name__]
        self.remote.parts = self._get_part_tables(remote_table)
        self.remote.main = remote_table

    @staticmethod
    def _get_part_tables(table):
        parts = dict()
        for name in dir(table):
            if name[0].isupper() and not name == "FlaggedForDeletion":
                attr = getattr(table, name)
                if isclass(attr) and issubclass(attr, dj.Part):
                    parts[name] = attr
        return parts

    def _set_up_outbound_table(self):
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

        OutboundTable.__name__ = self.table_cls.__name__ + "Outbound"
        outbound_schema = dj.schema("datajoint_outbound__" + self.remote.database, connection=self.remote.conn)
        self.remote.gate = outbound_schema(OutboundTable)

    def _initialize_local(self):
        try:
            self._spawn_local_table()
        except KeyError:
            try:
                self._set_up_local_table()
            except LostConnectionError:
                raise LostConnectionError("Initial setup of local table requires connection to remote host")

    def _spawn_local_table(self):
        local_tables = dict()
        self.local.spawn_missing_classes(context=local_tables)
        local_table = local_tables[self.table_cls.__name__]

        class LocalTable(local_table):
            link = self

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

        LocalTable.__name__ = local_table.__name__
        self.local.parts = self._get_part_tables(LocalTable)
        self.local.main = LocalTable

    def _set_up_local_table(self):
        class LocalTable(dj.Lookup):
            link = self
            _definition = None

            class FlaggedForDeletion(dj.Part):
                definition = """
                -> master
                """

            @property
            def definition(self):
                return self._definition

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
            local_part = type(
                name, (dj.Part,), dict(definition="-> master\n" + self._replace_stores(str(remote_part().heading)))
            )
            local_parts[name] = local_part

        for name, local_part in local_parts.items():
            setattr(LocalTable, name, local_part)

        LocalTable.__name__ = self.table_cls.__name__
        LocalTable._definition = f"""
            remote_host: varchar(64)
            remote_schema: varchar(64)
            {self._replace_stores(str(self.remote.main().heading))}
        """
        self.local.parts = local_parts
        self.local.main = self.local.schema(LocalTable)

    def _replace_stores(self, definition):
        for local_store, remote_store in self.stores.items():
            definition = definition.replace(remote_store, local_store)
        return definition

    def refresh(self):
        try:
            self._refresh()
        except LostConnectionError:
            warnings.warn("Couldn't refresh tables. Check connection to remote host")

    def _refresh(self):
        with self._transaction():
            self._pull_new_flags()
            self._refresh_ready_for_deletion()

    def _pull_new_flags(self):
        outbound_flags = self.remote.flagged_for_deletion.fetch(as_dict=True)
        outbound_flags = self._translate(outbound_flags, self.remote)
        self.local.main().FlaggedForDeletion().insert(self.local.main().proj() & outbound_flags, skip_duplicates=True)

    def _refresh_ready_for_deletion(self):
        primary_keys = self.local.main().proj().fetch(as_dict=True)
        primary_keys = self._translate(primary_keys, self.local)
        self.remote.gate().ReadyForDeletion().insert(
            self.remote.gate().FlaggedForDeletion() - primary_keys, skip_duplicates=True
        )

    @staticmethod
    def _translate(keys, host):
        translated = [{k: v for k, v in x.items() if k not in ("remote_host", "remote_schema")} for x in keys]
        translated = [{"remote_host": host.host, "remote_schema": host.database, **x} for x in translated]
        return translated

    def pull(self, restrictions):
        restrictions = dj.AndList(restrictions)
        flagged = self.remote.gate().FlaggedForDeletion() + self.remote.gate().ReadyForDeletion()
        if n_flagged := len(flagged & (self.remote.main() & restrictions)):
            warnings.warn(
                f"Some of the requested entities were not "
                f"pulled because they are flagged and/or ready for deletion (n = {n_flagged})"
            )
        self.refresh()
        primary_keys = (self.remote.main().proj() - flagged & restrictions).fetch(as_dict=True)
        with TemporaryDirectory() as temp_dir:
            main_entities = (self.remote.main() & primary_keys).fetch(as_dict=True, download_path=temp_dir)
            part_entities = {
                n: (p() & primary_keys).fetch(as_dict=True, download_path=temp_dir)
                for n, p in self.remote.parts.items()
            }
            with self._transaction():
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
    def _transaction(self):
        old_local_parts_conns = {n: p.connection for n, p in self.local.parts.items()}
        try:
            for part in self.local.parts.values():
                part.connection = self.local.conn
            with self.local.transaction, self.remote.transaction:
                yield
        finally:
            for name, part in self.local.parts.items():
                part.connection = old_local_parts_conns[name]
