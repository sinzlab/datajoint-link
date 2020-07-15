from typing import List, Dict, Any, Type

from datajoint import Part

from .entity import Entity, EntityPacket, EntityPacketCreator
from ..types import PrimaryKey


class SourceTableProxy:
    entity_packet_creator: EntityPacketCreator = None

    def __init__(self, table_factory, download_path):
        self.table_factory = table_factory
        self.download_path = download_path

    @property
    def primary_keys(self) -> List[PrimaryKey]:
        return self.table_factory().proj().fetch(as_dict=True)

    def get_primary_keys_in_restriction(self, restriction) -> List[PrimaryKey]:
        return (self.table_factory().proj() & restriction).fetch(as_dict=True)

    def fetch(self, primary_keys: List[PrimaryKey]) -> EntityPacket:
        return self.entity_packet_creator.create(
            primary_attrs=self.table_factory().heading.primary_key,
            master_entities=[self._fetch_master(key) for key in primary_keys],
            part_entities=[self._fetch_parts(key) for key in primary_keys],
        )

    def _fetch_master(self, primary_key: PrimaryKey) -> Dict[str, Any]:
        return (self.table_factory() & primary_key).fetch1(download_path=self.download_path)

    def _fetch_parts(self, primary_key: PrimaryKey) -> Dict[str, Any]:
        return {name: self._fetch_part(primary_key, part) for name, part in self.table_factory.parts.items()}

    def _fetch_part(self, primary_key: PrimaryKey, part: Type[Part]) -> Dict[str, Any]:
        return (part & primary_key).fetch(download_path=self.download_path, as_dict=True)

    def __repr__(self) -> str:
        return self.__class__.__qualname__ + "(" + repr(self.table_factory) + ")"


class LocalTableProxy(SourceTableProxy):
    @property
    def deletion_requested(self) -> List[PrimaryKey]:
        return self.table_factory().DeletionRequested.fetch(as_dict=True)

    def delete(self, primary_keys: List[PrimaryKey]) -> None:
        (self.table_factory() & primary_keys).delete()

    def insert(self, table_entities: List[Entity]) -> None:
        for table_entity in table_entities:
            self._insert_master(table_entity.master)
            self._insert_parts(table_entity.parts)

    def _insert_master(self, master_entity: Dict[str, Any]) -> None:
        self.table_factory().insert1(master_entity)

    def _insert_parts(self, part_entities: Dict[str, Any]) -> None:
        for name, entities in part_entities.items():
            self._insert_part(name, entities)

    def _insert_part(self, part_name: str, part_entities: Any) -> None:
        self.table_factory.parts[part_name].insert(part_entities)

    def start_transaction(self) -> None:
        self.table_factory().connection.start_transaction()

    def commit_transaction(self) -> None:
        self.table_factory().connection.commit_transaction()

    def cancel_transaction(self) -> None:
        self.table_factory().connection.cancel_transaction()


class OutboundTableProxy(LocalTableProxy):
    @property
    def deletion_approved(self) -> List[PrimaryKey]:
        return self.table_factory().DeletionApproved.fetch(as_dict=True)

    def approve_deletion(self, primary_keys: List[PrimaryKey]) -> None:
        self.table_factory().DeletionApproved.insert(primary_keys)
