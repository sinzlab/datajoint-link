from unittest.mock import MagicMock, create_autospec

import pytest

from link.use_cases import USE_CASES, AbstractGatewayLink, initialize_use_cases


@pytest.fixture
def identifiers():
    return ["identifier" + str(i) for i in range(10)]


@pytest.fixture
def source_identifiers(identifiers):
    return identifiers


@pytest.fixture
def outbound_identifiers(identifiers):
    return identifiers[:5]


@pytest.fixture
def local_identifiers(outbound_identifiers):
    return outbound_identifiers


@pytest.fixture
def gateway_link_spy(source_identifiers, outbound_identifiers, local_identifiers):
    spy = create_autospec(AbstractGatewayLink, instance=True)
    spy.source.identifiers = source_identifiers
    spy.outbound.identifiers = outbound_identifiers
    spy.local.identifiers = local_identifiers
    return spy


@pytest.fixture
def output_port_spies():
    return {n: MagicMock(name=n + "_output_port_spy") for n in USE_CASES}


@pytest.fixture
def initialized_use_cases(gateway_link_spy, output_port_spies):
    return initialize_use_cases(gateway_link_spy, output_port_spies)


@pytest.fixture
def use_case(request, initialized_use_cases):
    return initialized_use_cases[request.module.USE_CASE]
