"""Contains logic related to input/output handling to/from services."""
from __future__ import annotations

from functools import partial
from typing import Any, Callable, Generic, Protocol, TypeVar

from .services import Request, Response

_Response = TypeVar("_Response", bound=Response)


class ResponseRelay(Generic[_Response]):
    """A relay that makes the response of one service available to another."""

    def __init__(self) -> None:
        """Initialize the relay."""
        self._response: _Response | None = None

    def get_response(self) -> _Response:
        """Return the response of the relayed service."""
        assert self._response is not None
        return self._response

    def __call__(self, response: _Response) -> None:
        """Store the response of the relayed service."""
        self._response = response


_Request = TypeVar("_Request", bound=Request)

_Response_co = TypeVar("_Response_co", bound=Response, covariant=True)

_Request_contra = TypeVar("_Request_contra", bound=Request, contravariant=True)


class Service(Protocol[_Request_contra, _Response_co]):
    """Protocol for services."""

    def __call__(self, request: _Request_contra, *, output_port: Callable[[_Response_co], None], **kwargs: Any) -> None:
        """Execute the service."""


class ReturningService(Protocol[_Request_contra, _Response_co]):
    """Protocol for services that return their response."""

    def __call__(
        self, request: _Request_contra, *, output_port: Callable[[_Response_co], None], **kwargs: Any
    ) -> _Response_co:
        """Execute the service."""


def make_responsive(service: Service[_Request, _Response]) -> ReturningService[_Request, _Response]:
    """Create a version of the service that returns its response in addition to sending it to the output port."""
    relay: ResponseRelay[_Response] = ResponseRelay()
    service = partial(service, output_port=relay)

    def returning_service(request: _Request, *, output_port: Callable[[_Response], None], **kwargs: Any) -> _Response:
        service(request, **kwargs)
        output_port(relay.get_response())
        return relay.get_response()

    return returning_service
