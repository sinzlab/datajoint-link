from abc import ABC

from link.adapters import gateway


class TestAbstractReadOnlyGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractReadOnlyGateway, ABC)


class TestAbstractGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractGateway, ABC)

    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(gateway.AbstractGateway, gateway.AbstractReadOnlyGateway)


class TestAbstractSourceGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractSourceGateway, ABC)

    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(gateway.AbstractSourceGateway, gateway.AbstractReadOnlyGateway)


class TestAbstractOutboundGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractOutboundGateway, ABC)

    def test_if_subclass_of_abstract_flagged_gateway(self):
        assert issubclass(gateway.AbstractOutboundGateway, gateway.AbstractGateway)


class TestAbstractLocalGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractLocalGateway, ABC)

    def test_if_subclass_of_abstract_flagged_gateway(self):
        assert issubclass(gateway.AbstractLocalGateway, gateway.AbstractGateway)
