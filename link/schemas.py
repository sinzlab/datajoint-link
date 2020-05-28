"""This module contains custom classes based on the schema class from DataJoint"""
import os
from typing import Optional, Dict, Any

from datajoint.connection import Connection
from datajoint.schemas import Schema


class LazySchema:
    """A proxy around an instance of the DataJoint schema class that creates said instance in a lazy way.

    This class creates an instance of the DataJoint schema class if the "initialize" method is called or a non-existing
    attribute is accessed. After creation all non-existing attributes will be looked up on the created instance.

    Attributes:
        schema_kwargs: A dictionary containing keyword arguments and their values. It is used to create an instance
            of the DataJoint schema class when needed.
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
        host: Optional[str] = None
    ) -> None:
        if connection is not None and host is not None:
            raise ValueError("Expected either 'connection' or 'host', got both")
        self.schema_kwargs: Dict[str, Any] = dict(
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
        """Fully initializes the lazy schema."""
        if not self._is_initialized:
            self._initialize()

    def _initialize(self) -> None:
        if self._host is not None:
            self.schema_kwargs["connection"] = self._conn_cls(
                self._host, os.environ["REMOTE_DJ_USER"], os.environ["REMOTE_DJ_PASS"]
            )
        self._schema = self._schema_cls(**self.schema_kwargs)
        self._is_initialized = True

    def __getattr__(self, item: str) -> Any:
        self.initialize()
        return getattr(self._schema, item)
