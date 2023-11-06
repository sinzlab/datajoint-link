"""Contains the unit of work for links."""
from __future__ import annotations

from abc import ABC
from collections import deque
from types import TracebackType
from typing import Callable, Iterable, Iterator, Protocol

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.events import EntityStateChanged
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
        self._updates: deque[EntityStateChanged] = deque()
        self._events: deque[events.Event] = deque()
        self._seen: list[Entity] = []

    def __enter__(self) -> UnitOfWork:
        """Enter the context in which updates to entities can be made."""

        def augment_entity(entity: Entity) -> None:
            original = getattr(entity, "apply")
            augmented = augment_entity_apply(entity, original)
            setattr(entity, "apply", augmented)
            setattr(entity, "_is_expired", False)

        def augment_entity_apply(
            entity: Entity, original: Callable[[Operations], None]
        ) -> Callable[[Operations], None]:
            def augmented(operation: Operations) -> None:
                assert hasattr(entity, "_is_expired")
                if entity._is_expired:
                    raise RuntimeError("Can not apply operation to expired entity.")
                if entity not in self._seen:
                    self._seen.append(entity)
                current_state = entity.state
                original(operation)
                new_state = entity.state
                if current_state is new_state:
                    return
                transition = Transition(current_state, new_state)
                command = TRANSITION_MAP[transition]
                self._updates.append(events.EntityStateChanged(operation, entity.identifier, transition, command))

            return augmented

        self._link = self._gateway.create_link()
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
            self._gateway.apply([self._updates.popleft()])
        for entity in self._seen:
            while entity.events:
                self._events.append(entity.events.popleft())
        self.rollback()

    def rollback(self) -> None:
        """Throw away any not yet persisted updates."""
        if self._link is None:
            raise RuntimeError("Not available outside of context")
        for entity in self._link:
            setattr(entity, "_is_expired", True)
        self._updates.clear()
        self._seen.clear()

    def collect_new_events(self) -> Iterator[events.Event]:
        """Collect new events from entities."""
        if self._link is not None:
            raise RuntimeError("New events can not be collected when inside context")
        while self._events:
            yield self._events.popleft()
