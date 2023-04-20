from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from typing import Iterable, Iterator, Optional, Protocol, TypedDict, Union

import pytest

from dj_link.entities.abstract_gateway import AbstractEntityDTO, AbstractGateway
from dj_link.entities.link import Transfer
from dj_link.use_cases.gateway import GatewayLink


class IdentifierCreator(Protocol):
    def __call__(self, spec: Union[int, Iterable[int]]) -> list[str]:
        ...


@pytest.fixture
def create_identifiers() -> IdentifierCreator:
    def _create_identifiers(spec: Union[int, Iterable[int]]) -> list[str]:
        if isinstance(spec, int):
            indexes = list(range(spec))
        elif isinstance(spec, Iterable):
            indexes = list(spec)
        else:
            raise RuntimeError("Invalid type for 'spec'")
        return ["identifier" + str(i) for i in indexes]

    return _create_identifiers


@dataclass(frozen=True)
class FakeEntityDTO(AbstractEntityDTO):
    identifier: str
    identifier_only: bool = False

    def create_identifier_only_copy(self) -> FakeEntityDTO:
        return FakeEntityDTO(self.identifier, identifier_only=True)


class FakeGateway(AbstractGateway[FakeEntityDTO]):
    def __init__(self, entities: Optional[dict[str, FakeEntityDTO]] = None, *, identifier_only: bool = False) -> None:
        if entities is None:
            entities = {}
        self.__flags: dict[str, dict[str, bool]] = defaultdict(dict)
        self.__entities: dict[str, FakeEntityDTO] = entities
        self.__identifier_only = identifier_only

    def get_flags(self, identifier: str) -> dict[str, bool]:
        return self.__flags[identifier]

    def fetch(self, identifier: str) -> FakeEntityDTO:
        return self.__entities[identifier]

    def insert(self, entity_dto: FakeEntityDTO) -> None:
        assert self.__identifier_only is entity_dto.identifier_only, "Identifier only mismatch"
        self.__entities[entity_dto.identifier] = entity_dto

    def delete(self, identifier: str) -> None:
        del self.__entities[identifier]

    def set_flag(self, identifier: str, flag: str, value: bool) -> None:
        self.__flags[identifier][flag] = value

    def start_transaction(self) -> None:
        pass

    def commit_transaction(self) -> None:
        pass

    def cancel_transaction(self) -> None:
        pass

    def __len__(self) -> int:
        return len(self.__entities)

    def __iter__(self) -> Iterator[str]:
        return iter(self.__entities)

    @classmethod
    def from_identifiers(cls, identifiers: Iterable[str], identifier_only: bool = False) -> FakeGateway:
        return cls(
            {identifier: FakeEntityDTO(identifier) for identifier in identifiers}, identifier_only=identifier_only
        )


class GatewayLinkIdentifiers(TypedDict):
    source: Iterable[str]
    outbound: Iterable[str]
    local: Iterable[str]


class FakeGatewayLink(GatewayLink):
    def __init__(
        self,
        source: Optional[FakeGateway] = None,
        outbound: Optional[FakeGateway] = None,
        local: Optional[FakeGateway] = None,
    ) -> None:
        def validate_gateway(gateway: Optional[FakeGateway]) -> FakeGateway:
            if gateway is None:
                return FakeGateway()
            return gateway

        self.__source = validate_gateway(source)
        self.__outbound = validate_gateway(outbound)
        self.__local = validate_gateway(local)

    @property
    def source(self) -> FakeGateway:
        return self.__source

    @property
    def outbound(self) -> FakeGateway:
        return self.__outbound

    @property
    def local(self) -> FakeGateway:
        return self.__local

    @classmethod
    def from_identifiers(cls, identifiers: GatewayLinkIdentifiers) -> FakeGatewayLink:
        return FakeGatewayLink(
            source=FakeGateway.from_identifiers(identifiers["source"]),
            outbound=FakeGateway.from_identifiers(identifiers["outbound"], identifier_only=True),
            local=FakeGateway.from_identifiers(identifiers["local"]),
        )

    def transfer(self, spec: Transfer) -> None:
        raise NotImplementedError


@pytest.fixture
def fake_gateway_link() -> FakeGatewayLink:
    return FakeGatewayLink()


class FakeGatewayLinkCreator(Protocol):
    def __call__(self, identifiers: GatewayLinkIdentifiers) -> FakeGatewayLink:
        ...


@pytest.fixture
def create_fake_gateway_link() -> FakeGatewayLinkCreator:
    def _create_fake_gateway_link(identifiers: GatewayLinkIdentifiers) -> FakeGatewayLink:
        return FakeGatewayLink.from_identifiers(identifiers)

    return _create_fake_gateway_link
