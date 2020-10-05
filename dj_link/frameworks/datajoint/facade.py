from typing import List, Dict
from itertools import chain

from .file import ReusableTemporaryDirectory
from ...adapters.datajoint.abstract_facade import AbstractTableFacade
from ...adapters.datajoint.gateway import EntityDTO
from ...base import Base
from ...types import PrimaryKey


class TableFacade(AbstractTableFacade, Base):
    def __init__(self, table_factory, temp_dir: ReusableTemporaryDirectory) -> None:
        self.table_factory = table_factory
        self.temp_dir = temp_dir

    @property
    def primary_keys(self) -> List[PrimaryKey]:
        """Returns all primary keys present in the table."""
        return self.table_factory().proj().fetch(as_dict=True)

    def get_primary_keys_in_restriction(self, restriction) -> List[PrimaryKey]:
        """Gets the primary keys of all entities in the restriction."""
        return (self.table_factory().proj() & restriction).fetch(as_dict=True)

    def get_flags(self, primary_key: PrimaryKey) -> Dict[str, bool]:
        """Gets the flags of the entity identified by the provided primary key."""
        return {
            flag_table_name: primary_key in (flag_table & primary_key)
            for flag_table_name, flag_table in self.table_factory.flag_tables.items()
        }

    def fetch(self, primary_key: PrimaryKey) -> EntityDTO:
        """Fetches the entity identified by the provided primary key from the table."""
        table = self.table_factory()
        master_entity = (table & primary_key).fetch1(download_path=self.temp_dir.name)
        part_entities = {}
        for part_name, part in self.table_factory.part_tables.items():
            part_entities[part_name] = (part & primary_key).fetch(as_dict=True, download_path=self.temp_dir.name)
        return EntityDTO(table.primary_key, master_entity, parts=part_entities)

    def insert(self, entity_dto: EntityDTO) -> None:
        """Inserts the entity into the table."""
        self.table_factory().insert1(entity_dto.master)
        for part_name, part_entity in entity_dto.parts.items():
            self.table_factory.part_tables[part_name].insert(part_entity)

    def delete(self, primary_key: PrimaryKey) -> None:
        """Deletes the entity identified by the provided primary key from the table."""
        for part in chain(self.table_factory.part_tables.values(), self.table_factory.flag_tables.values()):
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
