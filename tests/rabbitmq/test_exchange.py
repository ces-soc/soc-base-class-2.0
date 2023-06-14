import pytest
from cessoc.rabbitmq.exchange import Exchange, ExchangeType


@pytest.fixture(scope="function")
def exchange_name():
    """Default exchange name"""
    return "test"


@pytest.fixture(scope="function")
def exchange(exchange_name):
    """Default exchange instance"""
    return Exchange(exchange_name)


class TestExchange:
    """Exchange Class Test Cases"""

    def test_init(self, exchange_name):
        """Test if the class instantiates"""
        Exchange(exchange_name)

    def test_eq_equal_same_instance(self, exchange):
        """Instance should equal itself"""
        assert exchange == exchange
        assert exchange is exchange

    def test_eq_equal_different_instance(self, exchange, exchange_name):
        """Two instances with same instance attributes should equal"""
        assert exchange == Exchange(exchange_name)

    def test_eq_is_not_same_instance(self, exchange, exchange_name):
        """Instance is itself"""
        assert exchange is not Exchange(exchange_name)

    def test_eq_not_equal_name(self, exchange):
        """Instances should not equal when name differs"""
        assert exchange != Exchange("test2")

    def test_eq_not_equal_type(self, exchange, exchange_name):
        """Instances should not equal when type differs"""
        assert exchange != Exchange(exchange_name, exchange_type=ExchangeType.FANOUT)

    def test_eq_not_equal_passive(self, exchange, exchange_name):
        """Instances should not equal when type differs"""
        assert exchange != Exchange(exchange_name, passive=True)

    def test_eq_not_equal_durable(self, exchange, exchange_name):
        """Instances should not equal when type differs"""
        assert exchange != Exchange(exchange_name, durable=True)

    def test_eq_not_equal_auto_delete(self, exchange, exchange_name):
        """Instances should not equal when type differs"""
        assert exchange != Exchange(exchange_name, auto_delete=False)

    def test_eq_not_equal_internal(self, exchange, exchange_name):
        """Instances should not equal when type differs"""
        assert exchange != Exchange(exchange_name, internal=True)

    def test_name_set_on_init(self):
        """Name should be set from constructor"""
        name = "test"
        ex = Exchange(name)
        assert ex.name == name

    def test_name_set_by_property(self, exchange):
        """Name should be set by property"""
        name = "test2"
        exchange.name = name
        assert exchange.name == name

    def test_name_all_possible_characters(self, exchange):
        """All possible valid characters"""
        name = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-_.:"
        exchange.name = name
        assert exchange.name == name

    def test_name_1_character(self, exchange):
        """1 valid character"""
        name = "a"
        exchange.name = name
        assert exchange.name == name

    def test_name_256_characters(self, exchange):
        """256 valid characters"""
        name = "a" * 256
        exchange.name = name
        assert exchange.name == name

    def test_invalid_name_bad_characters(self, exchange):
        """Invalid character input"""
        with pytest.raises(ValueError):
            exchange.name = "%"

    def test_invalid_name_0_characters(self, exchange):
        """Invalid character input"""
        with pytest.raises(ValueError):
            exchange.name = ""

    def test_invalid_name_257_characters(self, exchange):
        """Invalid character input"""
        with pytest.raises(ValueError):
            name = "a" * 257
            exchange.name = name
