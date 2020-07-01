from dataclasses import dataclass
from functools import wraps


@dataclass(frozen=True)
class Address:
    host: str
    database: str
    table: str


def _needs_configuration(kind):
    def decorator(method):
        @wraps(method)
        def wrapper(*args, **kwargs):
            if not args[0].is_configured:
                raise RuntimeError(f"Can't get {kind} address of not configured configuration")
            return method(*args, **kwargs)

        return wrapper

    return decorator


class Configuration:
    def __init__(self) -> None:
        self._table_name = None
        self._local_host_name = None
        self._local_database_name = None
        self._source_host_name = None
        self._source_database_name = None
        self._outbound_database_name = None
        self._outbound_table_name = None
        self._is_configured = False

    def configure(
        self,
        table_name: str,
        local_host_name: str,
        local_database_name: str,
        source_host_name: str,
        source_database_name: str,
        outbound_database_name: str,
        outbound_table_name: str,
    ) -> None:
        if self._is_configured:
            raise RuntimeError("Can't configure already configured configuration")
        self._table_name = table_name
        self._local_host_name = local_host_name
        self._local_database_name = local_database_name
        self._source_host_name = source_host_name
        self._source_database_name = source_database_name
        self._outbound_database_name = outbound_database_name
        self._outbound_table_name = outbound_table_name
        self._is_configured = True

    @property
    def is_configured(self) -> bool:
        return self._is_configured

    @property
    @_needs_configuration("local")
    def local_address(self) -> Address:
        return Address(self._local_host_name, self._local_database_name, self._table_name)

    @property
    @_needs_configuration("source")
    def source_address(self) -> Address:
        return Address(self._source_host_name, self._source_database_name, self._table_name)

    @property
    @_needs_configuration("outbound")
    def outbound_address(self) -> Address:
        return Address(self._source_host_name, self._outbound_database_name, self._outbound_table_name)

    def __repr__(self):
        if self.is_configured:
            info = [
                self._table_name,
                self._local_host_name,
                self._local_database_name,
                self._source_host_name,
                self._source_database_name,
                self._outbound_database_name,
                self._outbound_table_name,
            ]
            return self.__class__.__qualname__ + "().configure(" + ", ".join(info) + ")"
        return self.__class__.__qualname__ + "()"
