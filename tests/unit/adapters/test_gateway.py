from abc import ABC

from link.adapters import gateway


class TestAbstractGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractGateway, ABC)


class TestAbstractFlaggedGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractFlaggedGateway, ABC)

    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(gateway.AbstractFlaggedGateway, gateway.AbstractGateway)


class TestAbstractSourceGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractFlaggedGateway, ABC)

    def test_if_subclass_of_abstract_gateway(self):
        assert issubclass(gateway.AbstractSourceGateway, gateway.AbstractGateway)


class TestAbstractOutboundGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractOutboundGateway, ABC)

    def test_if_subclass_of_abstract_flagged_gateway(self):
        assert issubclass(gateway.AbstractOutboundGateway, gateway.AbstractFlaggedGateway)


class TestAbstractLocalGateway:
    def test_if_abstract_base_class(self):
        assert issubclass(gateway.AbstractLocalGateway, ABC)

    def test_if_subclass_of_abstract_flagged_gateway(self):
        assert issubclass(gateway.AbstractLocalGateway, gateway.AbstractFlaggedGateway)
