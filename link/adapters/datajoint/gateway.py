from typing import List, Dict, Any
from itertools import tee

from .proxy import AbstractTableProxy
from .identification import IdentificationTranslator
from ...entities.gateway import AbstractGateway
from ...entities.representation import represent


class DataJointGateway(AbstractGateway):
    def __init__(self, table_proxy: AbstractTableProxy, translator: IdentificationTranslator) -> None:
        self.table_proxy = table_proxy
        self.translator = translator

    @property
    def identifiers(self) -> List[str]:
        """Returns the identifiers of all entities in the table."""
        return [self.translator.to_identifier(primary_key) for primary_key in self.table_proxy.primary_keys]

    def get_flags(self, identifier: str) -> Dict[str, bool]:
        """Gets the names and values of all flags that are set on the entity identified by the provided identifier."""
        flags = self.table_proxy.get_flags(self.translator.to_primary_key(identifier))
        return {self.to_flag_name(flag_table_name): flag for flag_table_name, flag in flags.items()}

    def fetch(self, identifier: str) -> Any:
        """Fetches the entity identified by the provided identifier."""
        primary_key = self.translator.to_primary_key(identifier)
        return dict(master=self.table_proxy.fetch_master(primary_key), parts=self.table_proxy.fetch_parts(primary_key))

    def insert(self, data: Any) -> None:
        """Inserts the provided entity into the table."""
        self.table_proxy.insert_master(data["master"])
        self.table_proxy.insert_parts(data["parts"])

    def delete(self, identifier: str) -> None:
        """Deletes the entity identified by the provided identifier from the table."""
        primary_key = self.translator.to_primary_key(identifier)
        self.table_proxy.delete_parts(primary_key)
        self.table_proxy.delete_master(primary_key)

    def set_flag(self, identifier: str, flag: str, value: bool) -> None:
        """Sets the flag on the entity identified by the provided identifier to the provided value."""
        if value:
            self._enable_flag(identifier, flag)
        else:
            self._disable_flag(identifier, flag)

    def _enable_flag(self, identifier: str, flag: str) -> None:
        self.table_proxy.enable_flag(self.translator.to_primary_key(identifier), self._to_flag_table_name(flag))

    def _disable_flag(self, identifier: str, flag: str) -> None:
        self.table_proxy.disable_flag(self.translator.to_primary_key(identifier), self._to_flag_table_name(flag))

    def start_transaction(self) -> None:
        """Starts a transaction in the table."""
        self.table_proxy.start_transaction()

    def commit_transaction(self) -> None:
        """Commits a transaction in the table."""
        self.table_proxy.commit_transaction()

    def cancel_transaction(self) -> None:
        """Cancels a transaction in the table."""
        self.table_proxy.cancel_transaction()

    @staticmethod
    def to_flag_name(flag_table_name: str) -> str:
        """Translates the provided flag table name to the corresponding flag name."""
        indexes = [index for index, letter in enumerate(flag_table_name) if letter.isupper() and index != 0]
        starts, stops = tee(indexes + [len(flag_table_name)])
        next(stops, None)
        parts = ["_" + flag_table_name[start:stop] for start, stop in zip(starts, stops)]
        return "".join([flag_table_name[: indexes[0]]] + parts).lower()

    @staticmethod
    def _to_flag_table_name(flag_name: str) -> str:
        """Translates the provided flag name to the corresponding flag table name."""
        return "".join(part.title() for part in flag_name.split("_"))

    def __repr__(self) -> str:
        return represent(self, ["table_proxy", "translator"])
