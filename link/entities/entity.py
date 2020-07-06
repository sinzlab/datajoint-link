from __future__ import annotations
from dataclasses import dataclass


@dataclass
class Entity:
    identifier: str


@dataclass
class ManagedEntity(Entity):
    deletion_requested: bool


@dataclass
class SourceEntity(Entity):
    pass


@dataclass
class OutboundEntity(ManagedEntity):
    deletion_approved: bool


@dataclass
class LocalEntity(ManagedEntity):
    pass


class EntityCreator:
    _entity_cls = Entity

    def __init__(self, gateway):
        self.gateway = gateway

    def create_entities(self):
        # noinspection PyArgumentList
        return [self._entity_cls(**entity_args) for entity_args in self._entities_args]

    @property
    def _entities_args(self):
        return [dict(identifier=identifier) for identifier in self.gateway.identifiers]

    def __repr__(self):
        return self.__class__.__qualname__ + "(" + repr(self.gateway) + ")"


class ManagedEntityCreator(EntityCreator):
    _entity_cls = ManagedEntity

    @property
    def _entities_args(self):
        return self._add_flags(super()._entities_args, "deletion_requested")

    def _add_flags(self, entities_args, flag_name):
        for entity_args in entities_args:
            entity_args[flag_name] = (
                True if entity_args["identifier"] in getattr(self.gateway, flag_name + "_identifiers") else False
            )
        return entities_args


class SourceEntityCreator(EntityCreator):
    _entity_cls = SourceEntity


class OutboundEntityCreator(ManagedEntityCreator):
    _entity_cls = OutboundEntity

    @property
    def _entities_args(self):
        return self._add_flags(super()._entities_args, "deletion_approved")


class LocalEntityCreator(ManagedEntityCreator):
    _entity_cls = LocalEntity
