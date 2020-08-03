from typing import List, Dict, Any, Optional
from dataclasses import dataclass

from ...adapters.datajoint.abstract_facade import AbstractTableEntityDTO, AbstractTableFacade
from ...base import Base
from ...types import PrimaryKey


@dataclass
class TableEntityDTO(AbstractTableEntityDTO):
    primary_key = master_entity = part_entities = None

    def __init__(
        self, primary_key: PrimaryKey, master_entity: Dict[str, Any], part_entities: Optional[Dict[str, Any]] = None,
    ):
        self.primary_key = primary_key
        self.master_entity = master_entity
        self.part_entities = part_entities if part_entities is not None else dict()


class TableFacade(AbstractTableFacade, Base):
    def __init__(self, table_factory, download_path: str) -> None:
        self.table_factory = table_factory
        self.download_path = download_path

    @property
    def primary_keys(self) -> List[PrimaryKey]:
        """Returns all primary keys present in the table."""
        return self.table_factory().proj().fetch(as_dict=True)

    def get_primary_keys_in_restriction(self, restriction) -> List[PrimaryKey]:
        """Gets the primary keys of all entities in the restriction."""
        return (self.table_factory().proj() & restriction).fetch(as_dict=True)

    def get_flags(self, primary_key: PrimaryKey) -> Dict[str, bool]:
        """Gets the flags of the entity identified by the provided primary key."""
        flags = dict()
        for flag_table_name, flag_table in self.table_factory.flag_tables.items():
            flags[flag_table_name] = primary_key in (flag_table & primary_key)
        return flags

    def fetch(self, primary_key: PrimaryKey) -> TableEntityDTO:
        """Fetches the entity identified by the provided primary key from the table."""
        master_entity = (self.table_factory() & primary_key).fetch1(download_path=self.download_path)
        part_entities = dict()
        for part_name, part in self.table_factory.part_tables.items():
            part_entities[part_name] = (part & primary_key).fetch(as_dict=True, download_path=self.download_path)
        return TableEntityDTO(primary_key=primary_key, master_entity=master_entity, part_entities=part_entities)

    def insert(self, table_entity_dto: TableEntityDTO) -> None:
        """Inserts the entity into the table."""
        self.table_factory().insert1(table_entity_dto.master_entity)
        for part_name, part_entity in table_entity_dto.part_entities.items():
            self.table_factory.part_tables[part_name].insert(part_entity)

    def delete(self, primary_key: PrimaryKey) -> None:
        """Deletes the entity identified by the provided primary key from the table."""
        for part in self.table_factory.part_tables.values():
            (part & primary_key).delete_quick()
        (self.table_factory() & primary_key).delete_quick()

    def enable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        """Enables the provided flag on the entity identified by the provided primary key."""
        self.table_factory.flag_tables[flag_table].insert1(primary_key)

    def disable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        """Disables the provided flag on the entity identified by the provided primary_key."""
        (self.table_factory.flag_tables[flag_table] & primary_key).delete_quick()

    def start_transaction(self) -> None:
        """Starts a transaction."""
        self.table_factory().connection.start_transaction()

    def commit_transaction(self) -> None:
        """Commits a transaction."""
        self.table_factory().connection.commit_transaction()

    def cancel_transaction(self) -> None:
        """Cancels a transaction."""
        self.table_factory().connection.cancel_transaction()
