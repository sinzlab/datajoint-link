from .repository import ReadOnlyRepository
from ..adapters.gateway import AbstractSourceGateway


class SourceRepository(ReadOnlyRepository):
    gateway: AbstractSourceGateway
