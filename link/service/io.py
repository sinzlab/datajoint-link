"""Contains logic related to input/output handling to/from services."""
from __future__ import annotations

from typing import Any, Callable, Protocol, TypeVar

from .services import Request, Response

_Response_co = TypeVar("_Response_co", bound=Response, covariant=True)

_Request_contra = TypeVar("_Request_contra", bound=Request, contravariant=True)


class Service(Protocol[_Request_contra, _Response_co]):
    """Protocol for services."""

    def __call__(self, request: _Request_contra, *, output_port: Callable[[_Response_co], None], **kwargs: Any) -> None:
        """Execute the service."""
