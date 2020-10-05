from unittest.mock import create_autospec, call

import pytest

from dj_link.entities.abstract_gateway import AbstractEntityDTO


USE_CASE = "pull"


@pytest.fixture
def config():
    return {
        "identifiers": {"source": 10, "outbound": 5, "local": 5},
        "flags": {
            "outbound": {"deletion_requested": [], "deletion_approved": []},
            "local": {"deletion_requested": []},
        },
    }


@pytest.fixture(autouse=True)
def execute_pull(use_case, request_model, create_identifiers):
    use_case(request_model(create_identifiers(range(2, 7))))


@pytest.fixture
def pulled_identifiers(create_identifiers):
    return create_identifiers(range(5, 7))


@pytest.fixture
def pulled_data(pulled_identifiers):
    return {identifier: create_autospec(AbstractEntityDTO, instance=True) for identifier in pulled_identifiers}


@pytest.fixture
def gateway_link_spy(gateway_link_spy, pulled_data):
    gateway_link_spy.source.fetch.side_effect = pulled_data.values()
    return gateway_link_spy


def test_if_correct_data_is_fetched_from_source_gateway(gateway_link_spy, pulled_identifiers):
    assert gateway_link_spy.source.fetch.call_args_list == [call(i) for i in pulled_identifiers]


def test_if_correct_data_is_inserted_into_outbound_gateway(gateway_link_spy, pulled_data):
    pulled_outbound_data = {i: d.create_identifier_only_copy.return_value for i, d in pulled_data.items()}
    assert gateway_link_spy.outbound.insert.call_args_list == [call(d) for d in pulled_outbound_data.values()]


def test_if_correct_data_is_inserted_into_local_gateway(gateway_link_spy, pulled_data):
    assert gateway_link_spy.local.insert.call_args_list == [call(d) for d in pulled_data.values()]
