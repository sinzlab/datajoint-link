"""Contains the unit of work for links."""
from __future__ import annotations

from abc import ABC
from collections import defaultdict, deque
from types import TracebackType
from typing import Callable, Iterable, Protocol

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.link import Link
from link.domain.state import TRANSITION_MAP, Entity, Operations, Transition

from .gateway import LinkGateway


class SupportsLinkApply(Protocol):
    """Protocol for an object that supports applying operations to links."""

    def __call__(self, operation: Operations, *, requested: Iterable[Identifier]) -> Link:
        """Apply the operation to the link."""


class UnitOfWork(ABC):
    """Controls if and when updates to entities of a link are persisted."""

    def __init__(self, gateway: LinkGateway) -> None:
        """Initialize the unit of work."""
        self._gateway = gateway
        self._link: Link | None = None
        self._updates: dict[Identifier, deque[events.Update]] = defaultdict(deque)

    def __enter__(self) -> UnitOfWork:
        """Enter the context in which updates to entities can be made."""

        def augment_link(link: Link) -> None:
            original = getattr(link, "apply")
            augmented = augment_link_apply(link, original)
            object.__setattr__(link, "apply", augmented)
            object.__setattr__(link, "_is_expired", False)

        def augment_link_apply(current: Link, original: SupportsLinkApply) -> SupportsLinkApply:
            def augmented(operation: Operations, *, requested: Iterable[Identifier]) -> Link:
                assert hasattr(current, "_is_expired")
                if current._is_expired:
                    raise RuntimeError("Can not apply operation to expired link")
                self._link = original(operation, requested=requested)
                augment_link(self._link)
                object.__setattr__(current, "_is_expired", True)
                return self._link

            return augmented

        def augment_entity(entity: Entity) -> None:
            original = getattr(entity, "apply")
            augmented = augment_entity_apply(entity, original)
            object.__setattr__(entity, "apply", augmented)
            object.__setattr__(entity, "_is_expired", False)

        def augment_entity_apply(
            current: Entity, original: Callable[[Operations], Entity]
        ) -> Callable[[Operations], Entity]:
            def augmented(operation: Operations) -> Entity:
                assert hasattr(current, "_is_expired")
                if current._is_expired is True:
                    raise RuntimeError("Can not apply operation to expired entity")
                new = original(operation)
                store_update(operation, current, new)
                augment_entity(new)
                object.__setattr__(current, "_is_expired", True)
                return new

            return augmented

        def store_update(operation: Operations, current: Entity, new: Entity) -> None:
            assert current.identifier == new.identifier
            if current.state is new.state:
                return
            transition = Transition(current.state, new.state)
            self._updates[current.identifier].append(
                events.Update(operation, current.identifier, transition, TRANSITION_MAP[transition])
            )

        self._link = self._gateway.create_link()
        augment_link(self._link)
        for entity in self._link:
            augment_entity(entity)
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the context rolling back any not yet persisted updates."""
        self.rollback()
        self._link = None

    @property
    def link(self) -> Link:
        """Return the link object that is governed by this unit of work."""
        if self._link is None:
            raise RuntimeError("Not available outside of context")
        return self._link

    def commit(self) -> None:
        """Persist updates made to the link."""
        if self._link is None:
            raise RuntimeError("Not available outside of context")
        while self._updates:
            identifier, updates = self._updates.popitem()
            while updates:
                self._gateway.apply([updates.popleft()])
        self.rollback()

    def rollback(self) -> None:
        """Throw away any not yet persisted updates."""
        if self._link is None:
            raise RuntimeError("Not available outside of context")
        object.__setattr__(self._link, "_is_expired", True)
        for entity in self._link:
            object.__setattr__(entity, "_is_expired", True)
        self._updates.clear()
