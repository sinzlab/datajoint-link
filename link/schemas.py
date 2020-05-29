"""This module contains custom classes based on the "Schema" class from DataJoint"""
import os
from typing import Optional, Dict, Any, Type

from datajoint.connection import Connection
from datajoint.schemas import Schema
from datajoint.table import Table


class LazySchema:
    """A proxy for a "Schema" instance which creates said instance in a lazy way.

    This class creates a "Schema" instance if the "initialize" method is called or a non-existing attribute is accessed.
    After creation all non-existing attributes are looked up on the created instance.
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
            host: An optional address to a database server. If provided the "Schema" instance will be created with a
                connection to said database server. The user and password used to establish the connection must be
                present in the form of the environment variables "REMOTE_DJ_USER" and "REMOTE_DJ_PASS", respectively.
                Can not be passed together with a connection object.
        """
        if connection is not None and host is not None:
            raise ValueError("Expected either 'connection' or 'host', got both")
        self._schema_kwargs: Dict[str, Any] = dict(
            schema_name=schema_name,
            context=context,
            connection=connection,
            create_schema=create_schema,
            create_tables=create_tables,
        )
        self._host = host
        self._is_initialized = False
        self._schema: Optional[Schema] = None

    def initialize(self) -> None:
        """Creates a "Schema" instance if it was not already created."""
        if not self._is_initialized:
            self._initialize()

    def _initialize(self) -> None:
        if self._host is not None:
            self._schema_kwargs["connection"] = self._conn_cls(
                self._host, os.environ["REMOTE_DJ_USER"], os.environ["REMOTE_DJ_PASS"]
            )
        self._schema = self._schema_cls(**self._schema_kwargs)
        self._is_initialized = True

    @property
    def is_initialized(self) -> bool:
        return self._is_initialized

    def __getattr__(self, item: str) -> Any:
        self.initialize()
        return getattr(self._schema, item)

    def __call__(self, cls: Type[Table], *, context: Dict[str, Any] = None) -> Type[Table]:
        self.initialize()
        return self._schema(cls, context=context)

    def __repr__(self) -> str:
        self.initialize()
        return f"{self.__class__.__qualname__}({repr(self._schema)})"
