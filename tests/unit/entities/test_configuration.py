import dataclasses
import pytest

from link.entities import configuration


class TestAddress:
    def test_if_address_is_dataclass(self):
        assert dataclasses.is_dataclass(configuration.Address)

    def test_if_address_is_frozen(self):
        address = configuration.Address("host", "database", "table")
        with pytest.raises(dataclasses.FrozenInstanceError):
            # noinspection PyDataclass
            address.host = "host2"


@pytest.fixture
def table_name():
    return "table"


@pytest.fixture
def local_host_name():
    return "local_host"


@pytest.fixture
def local_database_name():
    return "local_database"


@pytest.fixture
def source_host_name():
    return "source_host"


@pytest.fixture
def source_database_name():
    return "source_database"


@pytest.fixture
def outbound_database_name():
    return "outbound_database"


@pytest.fixture
def outbound_table_name():
    return "outbound_table"


@pytest.fixture
def info(
    table_name,
    local_host_name,
    local_database_name,
    source_host_name,
    source_database_name,
    outbound_database_name,
    outbound_table_name,
):
    return [
        table_name,
        local_host_name,
        local_database_name,
        source_host_name,
        source_database_name,
        outbound_database_name,
        outbound_table_name,
    ]


@pytest.fixture
def config():
    return configuration.Configuration()


@pytest.fixture
def configure(config, info):
    def _configure():
        config.configure(*info)

    _configure()
    return _configure


def test_if_is_configured_flag_is_false_if_not_configured(config):
    assert config.is_configured is False


class TestConfigure:
    @pytest.mark.usefixtures("configure")
    def test_if_is_configured_flag_is_switched(self, config):
        assert config.is_configured is True

    def test_if_runtime_error_is_raised_if_already_configured(self, config, configure):
        with pytest.raises(RuntimeError):
            configure()


class TestLocalAddressProperty:
    def test_if_runtime_error_is_raised_if_not_configured(self, config):
        with pytest.raises(RuntimeError):
            _ = config.local_address

    @pytest.mark.usefixtures("configure")
    def test_if_correct_address_is_returned(self, config, table_name, local_host_name, local_database_name):
        assert config.local_address == configuration.Address(local_host_name, local_database_name, table_name)


class TestSourceAddressProperty:
    def test_if_runtime_error_is_raised_if_not_configured(self, config):
        with pytest.raises(RuntimeError):
            _ = config.source_address

    @pytest.mark.usefixtures("configure")
    def test_if_correct_address_is_returned(self, config, table_name, source_host_name, source_database_name):
        assert config.source_address == configuration.Address(source_host_name, source_database_name, table_name)


class TestOutboundAddressProperty:
    def test_if_runtime_error_is_raised_not_configured(self, config):
        with pytest.raises(RuntimeError):
            _ = config.outbound_address

    @pytest.mark.usefixtures("configure")
    def test_if_correct_address_is_returned(
        self, config, outbound_table_name, source_host_name, outbound_database_name
    ):
        assert config.outbound_address == configuration.Address(
            source_host_name, outbound_database_name, outbound_table_name
        )


class TestRepr:
    def test_while_not_configured(self, config):
        assert repr(config) == "Configuration()"

    @pytest.mark.usefixtures("configure")
    def test_while_configured(self, config, info):
        assert repr(config) == f"Configuration().configure(" + ", ".join(info) + ")"
