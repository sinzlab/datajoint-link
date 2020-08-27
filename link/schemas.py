"""This module contains custom classes based on the "Schema" class from DataJoint"""
import os
from typing import Optional, Dict, Any, Type

from datajoint.connection import Connection
from datajoint.schemas import Schema
from datajoint.table import Table


class LazySchema:
    """A proxy for a "Schema" instance which initializes said instance in a lazy way.

    This class initializes the underlying schema if the "initialize" or "__call__" method is called or the "schema"
    attribute is accessed. Trying to access attributes that do not exist on this class will lead to initialization of
    the underlying schema and subsequent lookup of the requested attribute on the now initialized schema.

    Attributes:
        database: The name of the associated database schema.
        context: None or a dictionary used to look up foreign key references.
        connection: None or a connection object. This attribute can not be set while the "host" attribute is set.
        create_schema: When "False", do not create the schema in the database if missing on initialization and raise an
            error.
        create_tables: When "False", do not create missing tables in the schema and raise an error.
        host: None or an address to a database server. The underlying schema instance will be initialized with a
            connection to the database server found at the address if this attribute is set during initialization.
            In this case the username and password used to establish the connection are taken from the environment
            variables called "REMOTE_DJ_USER" and "REMOTE_DJ_PASS", respectively. This attribute can not be set while
            the "connection" attribute is set.
        is_initialized: "True" if the underlying schema is initialized, "False" otherwise.
        schema: The underlying schema object. Accessing this attribute will initialize said schema object if it is not
            already initialized.
    """

    _schema_cls = Schema
    _conn_cls = Connection

    def __init__(
        self,
        schema_name: str,
        context: Optional[Dict] = None,
        *,
        connection: Optional[Connection] = None,
        create_schema: Optional[bool] = True,
        create_tables: Optional[bool] = True,
        host: Optional[str] = None,
    ) -> None:
        """Initializes an instance of "LazySchema".

        Args:
            schema_name: The name of the database schema to associate.
            context: An optional dictionary for looking up foreign key references.
            connection: An optional connection object. Can not be passed together with a host address.
            create_schema: When "False", do not create the schema on the database if missing and raise an error.
            create_tables: When "False", do not create missing tables in the schema and raise an error.
            host: An optional address to a database server.
        """
        if connection is not None and host is not None:
            raise ValueError("Expected either 'connection' or 'host', got both")
        self.database = schema_name
        self.context = context
        self._connection = connection
        self.create_schema = create_schema
        self.create_tables = create_tables
        self._host = host
        self._is_initialized = False
        self._schema: Optional[Schema] = None

    @property
    def connection(self) -> Optional[Connection]:
        self.initialize()
        return self._connection

    @property
    def schema(self) -> Schema:
        self.initialize()
        return self._schema

    def initialize(self) -> None:
        """Initializes the underlying schema if it is not already initialized."""
        if not self._is_initialized:
            self._initialize()

    def _initialize(self) -> None:
        if self._host is not None:
            self._connection = self._conn_cls(self._host, os.environ["LINK_USER"], os.environ["LINK_PASS"])
        self._schema = self._schema_cls(
            schema_name=self.database,
            context=self.context,
            connection=self._connection,
            create_schema=self.create_schema,
            create_tables=self.create_tables,
        )
        self._connection = self._schema.connection
        self._is_initialized = True

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    def __getattr__(self, item: str) -> Any:
        return getattr(self.schema, item)

    def __call__(self, cls: Type[Table], *, context: Dict[str, Any] = None) -> Type[Table]:
        return self.schema(cls, context=context)

    def __repr__(self) -> str:
        return (
            f"{self.__class__.__qualname__}"
            f"({self.database}, context={self.context}, connection={self._connection}, "
            f"create_schema={self.create_schema}, create_tables={self.create_tables})"
        )
