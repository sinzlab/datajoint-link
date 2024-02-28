"""Contains the unit of work for links."""
from __future__ import annotations

from abc import ABC
from collections import deque
from types import TracebackType
from typing import Callable, Iterator

from link.domain import events
from link.domain.custom_types import Identifier
from link.domain.state import TRANSITION_MAP, Entity, Operations, Transition

from .gateway import LinkGateway


class UnitOfWork(ABC):
    """Controls if and when updates to entities of a link are persisted."""

    def __init__(self, gateway: LinkGateway) -> None:
        """Initialize the unit of work."""
        self._gateway = self._augment_gateway(gateway)
        self._entities: LinkGateway | None = None
        self._updates: deque[events.StateChanged] = deque()
        self._events: deque[events.Event] = deque()
        self._seen: list[Entity] = []

    def _augment_gateway(self, gateway: LinkGateway) -> LinkGateway:
        def augment_create_entity(original: Callable[[Identifier], Entity]) -> Callable[[Identifier], Entity]:
            def augmented(identifier: Identifier) -> Entity:
                entity = original(identifier)
                if entity not in self._seen:
                    self._seen.append(entity)
                self._augment_entity(entity)
                return entity

            return augmented

        original = getattr(gateway, "create_entity")
        augmented = augment_create_entity(original)
        setattr(gateway, "create_entity", augmented)
        return gateway

    def _augment_entity(self, entity: Entity) -> None:
        def augment_entity_apply(
            entity: Entity, original: Callable[[Operations], None]
        ) -> Callable[[Operations], None]:
            def augmented(operation: Operations) -> None:
                assert hasattr(entity, "_is_expired")
                if entity._is_expired:
                    raise RuntimeError("Can not apply operation to expired entity.")
                current_state = entity.state
                original(operation)
                new_state = entity.state
                if current_state is new_state:
                    return
                transition = Transition(current_state, new_state)
                command = TRANSITION_MAP[transition]
                self._updates.append(events.StateChanged(operation, entity.identifier, transition, command))

            return augmented

        original = getattr(entity, "apply")
        augmented = augment_entity_apply(entity, original)
        setattr(entity, "apply", augmented)
        setattr(entity, "_is_expired", False)

    def __enter__(self) -> UnitOfWork:
        """Enter the context in which updates to entities can be made."""
        self._entities = self._gateway
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the context rolling back any not yet persisted updates."""
        self.rollback()
        self._entities = None

    @property
    def entities(self) -> LinkGateway:
        """Return the link object that is governed by this unit of work."""
        if self._entities is None:
            raise RuntimeError("Not available outside of context")
        return self._entities

    def commit(self) -> None:
        """Persist updates made to the link."""
        if self._entities is None:
            raise RuntimeError("Not available outside of context")
        while self._updates:
            self._gateway.apply([self._updates.popleft()])
        for entity in self._seen:
            while entity.events:
                self._events.append(entity.events.popleft())
        self.rollback()

    def rollback(self) -> None:
        """Throw away any not yet persisted updates."""
        if self._entities is None:
            raise RuntimeError("Not available outside of context")
        for entity in self._seen:
            setattr(entity, "_is_expired", True)
        self._updates.clear()
        self._seen.clear()

    def collect_new_events(self) -> Iterator[events.Event]:
        """Collect new events from entities."""
        if self._entities is not None:
            raise RuntimeError("New events can not be collected when inside context")
        while self._events:
            yield self._events.popleft()
