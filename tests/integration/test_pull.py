from typing import Callable, Generic, Iterable, Optional

import pytest

from dj_link.use_cases import RepositoryLinkFactory
from dj_link.use_cases.base import ResponseModel
from dj_link.use_cases.gateway import GatewayLink
from dj_link.use_cases.pull import PullRequestModel, PullResponseModel, PullUseCase

from ..conftest import FakeGatewayLinkCreator


class FakeOutputPort(Generic[ResponseModel]):
    """Remembers the last response that was passed to it."""

    def __init__(self) -> None:
        self.response: Optional[ResponseModel] = None

    def __call__(self, response: ResponseModel) -> None:
        self.response = response


@pytest.fixture
def fake_output_port() -> FakeOutputPort[PullResponseModel]:
    return FakeOutputPort()


def pull(
    gateway_link: GatewayLink, output_port: Callable[[PullResponseModel], None], *, requested: Iterable[str]
) -> None:
    use_case = PullUseCase(gateway_link, RepositoryLinkFactory(gateway_link), output_port)
    request = PullRequestModel(list(requested))
    use_case(request)


def test_correct_entities_are_pulled(
    create_fake_gateway_link: FakeGatewayLinkCreator,
    fake_output_port: FakeOutputPort[PullResponseModel],
) -> None:
    fake_gateway_link = create_fake_gateway_link(
        {"source": {"apple", "banana", "grapefruit"}, "outbound": {"banana"}, "local": {"banana"}}
    )
    pull(fake_gateway_link, fake_output_port, requested={"apple", "banana"})
    assert set(fake_gateway_link.outbound) == set(fake_gateway_link.local) == {"apple", "banana"}


def test_correct_response_model_is_sent_to_output_port(
    create_fake_gateway_link: FakeGatewayLinkCreator, fake_output_port: FakeOutputPort[PullResponseModel]
) -> None:
    fake_gateway_link = create_fake_gateway_link(
        {"source": {"apple", "banana", "grapefruit"}, "outbound": {"banana"}, "local": {"banana"}}
    )
    pull(fake_gateway_link, fake_output_port, requested={"apple", "banana"})
    assert fake_output_port.response == PullResponseModel(
        requested={"apple", "banana"}, valid={"apple"}, invalid={"banana"}
    )