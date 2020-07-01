from .repository import Repository
from ..adapters.gateway import AbstractSourceGateway


class SourceRepository(Repository):
    gateway: AbstractSourceGateway
