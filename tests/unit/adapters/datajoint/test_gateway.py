from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any, Mapping, Optional, Union, overload

import pytest

from dj_link.adapters.datajoint.abstract_facade import AbstractTableFacade
from dj_link.adapters.datajoint.gateway import DataJointGateway, DataJointGatewayLink, EntityDTO
from dj_link.adapters.datajoint.identification import IdentificationTranslator
from dj_link.custom_types import PrimaryKey
from dj_link.entities.link import Components, Identifier, Transfer


@dataclass
class Flag:
    primary_key: PrimaryKey
    flag_table: str
    is_enabled: bool


class FakeTableFacade(AbstractTableFacade):
    def __init__(self, entities: Optional[Iterable[EntityDTO]] = None) -> None:
        if entities is None:
            entities = []
        self.__entities: list[EntityDTO] = list(entities)
        self.__flags: list[Flag] = []

    def get_primary_keys_in_restriction(self, restriction: Any) -> list[PrimaryKey]:
        name, raw_value = (s.strip() for s in restriction.split("="))
        value = int(raw_value)
        result = []
        for entity in self.__entities:
            primary_key = self.__construct_primary_key(entity)
            if primary_key[name] != value:
                continue
            result.append(primary_key)
        return result

    def get_flags(self, primary_key: PrimaryKey) -> dict[str, bool]:
        return {f.flag_table: f.is_enabled for f in self.__flags if f.primary_key == primary_key}

    @staticmethod
    def __construct_primary_key(entity: EntityDTO) -> PrimaryKey:
        return {k: entity.master[k] for k in entity.primary_key}

    def fetch(self, primary_key: PrimaryKey) -> EntityDTO:
        for entity in self.__entities:
            if primary_key == self.__construct_primary_key(entity):
                return entity
        raise KeyError

    def insert(self, entity_dto: EntityDTO) -> None:
        if entity_dto in self.__entities:
            raise ValueError
        self.__entities.append(entity_dto)

    def delete(self, primary_key: PrimaryKey) -> None:
        for i, entity in enumerate(self.__entities):
            if primary_key == self.__construct_primary_key(entity):
                break
        else:
            raise KeyError
        del self.__entities[i]

    def enable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        try:
            flag = next(f for f in self.__flags if f.primary_key == primary_key and f.flag_table == flag_table)
        except StopIteration:
            self.__flags.append(Flag(primary_key, flag_table, is_enabled=True))
        else:
            flag.is_enabled = True

    def disable_flag(self, primary_key: PrimaryKey, flag_table: str) -> None:
        try:
            flag = next(f for f in self.__flags if f.primary_key == primary_key and f.flag_table == flag_table)
        except StopIteration:
            self.__flags.append(Flag(primary_key, flag_table, is_enabled=False))
        else:
            flag.is_enabled = False

    def start_transaction(self) -> None:
        raise NotImplementedError

    def commit_transaction(self) -> None:
        raise NotImplementedError

    def cancel_transaction(self) -> None:
        raise NotImplementedError

    def __len__(self) -> int:
        return len(self.__entities)

    def __iter__(self) -> Iterator[PrimaryKey]:
        for entity in self.__entities:
            yield self.__construct_primary_key(entity)


def test_fetch_raises_key_error_if_entity_is_missing() -> None:
    gateway = DataJointGateway(FakeTableFacade(), IdentificationTranslator())
    with pytest.raises(KeyError):
        gateway.fetch("identifier")


Entity = Mapping[str, int]


@overload
def create_translations(
    primary_key_attributes: Iterable[str],
    entities: Entity,
) -> tuple[IdentificationTranslator, str, EntityDTO]:
    ...


@overload
def create_translations(
    primary_key_attributes: Iterable[str],
    entities: Iterable[Entity],
) -> tuple[IdentificationTranslator, list[str], list[EntityDTO]]:
    ...


def create_translations(
    primary_key_attributes: Iterable[str],
    entities: Union[Entity, Iterable[Entity]],
) -> tuple[IdentificationTranslator, Union[str, list[str]], Union[EntityDTO, list[EntityDTO]]]:
    if isinstance(entities, Mapping):
        entities = [entities]
    else:
        entities = list(entities)
    translator = IdentificationTranslator()
    primary_keys = [{k: v for k, v in entity.items() if k in primary_key_attributes} for entity in entities]
    identifiers = [translator.to_identifier(primary_key) for primary_key in primary_keys]
    dtos = [EntityDTO(list(primary_key), dict(entity)) for primary_key, entity in zip(primary_keys, entities)]
    if len(entities) == 1:
        return translator, identifiers[0], dtos[0]
    return translator, identifiers, dtos


def test_fetch_returns_correct_entity() -> None:
    translator, identifier, dto = create_translations("ab", {"a": 1, "b": 2, "c": 3})
    gateway = DataJointGateway(FakeTableFacade(), translator)
    gateway.insert(dto)
    assert gateway.fetch(identifier) == dto


def test_can_delete_entity() -> None:
    translator, identifier, dto = create_translations("ab", {"a": 1, "b": 2, "c": 3})
    gateway = DataJointGateway(FakeTableFacade(), translator)
    gateway.insert(dto)
    gateway.delete(identifier)
    with pytest.raises(KeyError):
        gateway.fetch(identifier)


def test_if_iteration_yields_correct_identifiers() -> None:
    translator, identifiers, dtos = create_translations(
        "ab", [{"a": 1, "b": 2, "c": 3}, {"a": 3, "b": 4, "c": 3}, {"a": 2, "b": 4, "c": 3}]
    )
    gateway = DataJointGateway(FakeTableFacade(), translator)
    for dto in dtos:
        gateway.insert(dto)
    assert set(gateway) == set(identifiers)


def test_if_length_is_correct() -> None:
    translator, _, dtos = create_translations(
        "ab", [{"a": 1, "b": 2, "c": 3}, {"a": 3, "b": 4, "c": 3}, {"a": 2, "b": 4, "c": 3}]
    )
    gateway = DataJointGateway(FakeTableFacade(), translator)
    for dto in dtos:
        gateway.insert(dto)
    assert len(gateway) == 3


@pytest.mark.parametrize("is_enabled", [True, False])
def test_can_set_flag(is_enabled: bool) -> None:
    translator, identifier, dto = create_translations("ab", {"a": 1, "b": 2, "c": 3})
    gateway = DataJointGateway(FakeTableFacade(), translator)
    gateway.insert(dto)
    gateway.set_flag(identifier, "some_flag", is_enabled)
    assert gateway.get_flags(identifier) == {"some_flag": is_enabled}


def test_can_get_identifiers_in_restriction() -> None:
    translator, identifiers, dtos = create_translations(
        "ab", [{"a": 1, "b": 2, "c": 3}, {"a": 1, "b": 3, "c": 3}, {"a": 5, "b": 1, "c": 3}]
    )
    gateway = DataJointGateway(FakeTableFacade(), translator)
    for dto in dtos:
        gateway.insert(dto)
    assert set(gateway.get_identifiers_in_restriction("a = 1")) == set(identifiers[:2])


def test_can_transfer_entity() -> None:
    translator, identifier, dto = create_translations("ab", {"a": 1, "b": 2, "c": 3})
    facades = {"source": FakeTableFacade(), "outbound": FakeTableFacade(), "local": FakeTableFacade()}
    gateways = {c: DataJointGateway(f, translator) for c, f in facades.items()}
    link = DataJointGatewayLink(**gateways)
    gateways["source"].insert(dto)
    spec = Transfer(
        Identifier(identifier), origin=Components.SOURCE, destination=Components.LOCAL, identifier_only=False
    )

    link.transfer(spec)

    assert gateways["local"].fetch(identifier) == dto


def test_can_transfer_identifier_only() -> None:
    primary_key = {"a": 1, "b": 2}
    translator, identifier, dto = create_translations("ab", dict(**primary_key, c=3))
    facades = {"source": FakeTableFacade(), "outbound": FakeTableFacade(), "local": FakeTableFacade()}
    gateways = {c: DataJointGateway(f, translator) for c, f in facades.items()}
    link = DataJointGatewayLink(**gateways)
    gateways["source"].insert(dto)
    spec = Transfer(
        Identifier(identifier), origin=Components.SOURCE, destination=Components.OUTBOUND, identifier_only=True
    )

    link.transfer(spec)

    assert gateways["outbound"].fetch(identifier) == EntityDTO(list(primary_key), primary_key)
