"""Contains the unit of work for links."""
from __future__ import annotations

from abc import ABC
from collections import defaultdict, deque
from types import TracebackType
from typing import Callable

from link.domain.custom_types import Identifier
from link.domain.link import Link
from link.domain.state import TRANSITION_MAP, Entity, Operations, Transition, Update

from .gateway import LinkGateway


class UnitOfWork(ABC):
    """Controls if and when updates to entities of a link are persisted."""

    def __init__(self, gateway: LinkGateway) -> None:
        """Initialize the unit of work."""
        self._gateway = gateway
        self._link: Link | None = None
        self._updates: dict[Identifier, deque[Update]] = defaultdict(deque)
        self._entities: dict[Identifier, Entity] = {}

    def __enter__(self) -> UnitOfWork:
        """Enter the context in which updates to entities can be made."""

        def track_entity(entity: Entity) -> None:
            apply = getattr(entity, "apply")
            augmented = augment_apply(entity, apply)
            object.__setattr__(entity, "apply", augmented)
            object.__setattr__(entity, "_is_expired", False)
            self._entities[entity.identifier] = entity

        def augment_apply(current: Entity, apply: Callable[[Operations], Entity]) -> Callable[[Operations], Entity]:
            def track_and_apply(operation: Operations) -> Entity:
                assert hasattr(current, "_is_expired")
                if current._is_expired is True:
                    raise RuntimeError("Can not apply operation to expired entity")
                new = apply(operation)
                store_update(operation, current, new)
                track_entity(new)
                return new

            return track_and_apply

        def store_update(operation: Operations, current: Entity, new: Entity) -> None:
            assert current.identifier == new.identifier
            if current.state is new.state:
                return
            transition = Transition(current.state, new.state)
            self._updates[current.identifier].append(
                Update(operation, current.identifier, transition, TRANSITION_MAP[transition])
            )

        link = self._gateway.create_link()
        for entity in link:
            track_entity(entity)
        self._link = link
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the context rolling back any not yet persisted updates."""
        self.rollback()

    @property
    def link(self) -> Link:
        """Return the link object that is governed by this unit of work."""
        if self._link is None:
            raise RuntimeError("Not available outside of context")
        return self._link

    def commit(self) -> None:
        """Persist updates made to the link."""
        while self._updates:
            identifier, updates = self._updates.popitem()
            while updates:
                self._gateway.apply([updates.popleft()])
        self.rollback()

    def rollback(self) -> None:
        """Throw away any not yet persisted updates."""
        self._link = None
        for entity in self._entities.values():
            object.__setattr__(entity, "_is_expired", True)
        self._entities = {}
        self._updates = defaultdict(deque)
