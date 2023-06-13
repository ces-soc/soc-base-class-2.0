import pytest
from cessoc.rabbitmq.queue import Queue, QueueArguments, QueueDefinitionManager
from cessoc.rabbitmq.exchange import Exchange, ExchangeType

@pytest.fixture(scope="function")
def queue_arguments():
    """Returns queue arguments"""
    return QueueArguments()


@pytest.fixture(scope="function")
def queue_name():
    """Default queue name"""
    return "test"


@pytest.fixture(scope="function")
def queue(queue_name):
    """Default queue instance"""
    return Queue(queue_name)


@pytest.fixture(scope="function")
def queue_manager():
    """Default Queue Definition Manager instance"""
    return QueueDefinitionManager()


@pytest.fixture(scope="function")
def exchange_name():
    """Default exchange name"""
    return "test"


@pytest.fixture(scope="function")
def exchange(exchange_name):
    """Default exchange instance"""
    return Exchange(exchange_name, ExchangeType.FANOUT)


class TestQueueArguments:
    """QueueArguments Class Test Cases"""

    def test_init(self):
        """Test if the class instantiates"""
        QueueArguments()

    def test_eq_equal_same_instance(self, queue_arguments):
        """Instance should equal itself"""
        assert queue_arguments == queue_arguments
        assert queue_arguments is queue_arguments

    def test_eq_equal_different_instance(self, queue_arguments):
        """Two instances with same instance attributes should equal"""
        assert queue_arguments == QueueArguments()

    def test_eq_is_not_same_instance(self, queue_arguments):
        """Instance is itself"""
        assert queue_arguments is not QueueArguments()

    def test_eq_not_equal_max_priority(self, queue_arguments):
        """Instances should not equal when max priority differs"""
        assert queue_arguments != QueueArguments(max_priority=1)

    def test_get_arguments_empty(self, queue_arguments):
        """Arguments should return a dictionary with certain class attributes"""
        assert queue_arguments.arguments == {}

    def test_get_arguments_max_priority(self, queue_arguments):
        """Arguments should return a dictionary with certain class attributes"""
        queue_arguments.max_priority = 1
        assert queue_arguments.arguments == {"x-max-priority": 1}


class TestQueue:
    """Queue Class Test Cases"""

    def test_init(self, queue_name):
        """Test if the class instantiates"""
        Queue(queue_name)

    def test_eq_equal_same_instance(self, queue):
        """Instance should equal itself"""
        assert queue == queue
        assert queue is queue

    def test_eq_equal_different_instance(self, queue, queue_name):
        """Two instances with same instance attributes should equal"""
        assert queue == Queue(queue_name)

    def test_eq_is_not_same_instance(self, queue, queue_name):
        """Instance is itself"""
        assert queue is not Queue(queue_name)

    def test_name_set_on_init(self):
        """Name should be set from constructor"""
        name = "test"
        ex = Queue(name)
        assert ex.name == name

    def test_name_set_by_property(self, queue):
        """Name should be set by property"""
        name = "test2"
        queue.name = name
        assert queue.name == name

    def test_eq_not_equal_name(self, queue):
        """Instances should not equal when name differs"""
        assert queue != Queue("test1")

    def test_eq_not_equal_bindings(self, queue, queue_name):
        """Instances should not equal when bindings differs"""
        assert queue != Queue(queue_name, bindings={"test": lambda: "test" + "test"})

    def test_eq_not_equal_consumer_tag(self, queue, queue_name):
        """Instances should not equal when consumer_tag differs"""
        assert queue != Queue(queue_name, consumer_tag="test")

    def test_eq_not_equal_consume(self, queue, queue_name):
        """Instances should not equal when consume differs"""
        q1 = Queue(queue_name)
        q2 = Queue(queue_name, consume=True)
        assert q1 != q2

    def test_eq_not_equal_passive(self, queue, queue_name):
        """Instances should not equal when passive differs"""
        assert queue != Queue(queue_name, passive=True)

    def test_eq_not_equal_durable(self, queue, queue_name):
        """Instances should not equal when durable differs"""
        assert queue != Queue(queue_name, durable=True)

    def test_eq_not_equal_exclusive(self, queue, queue_name):
        """Instances should not equal when exclusive differs"""
        assert queue != Queue(queue_name, exclusive=True)

    def test_eq_not_equal_auto_delete(self, queue, queue_name):
        """Instances should not equal when auto_delete differs"""
        assert queue != Queue(queue_name, auto_delete=True)

    def test_eq_not_equal_arguments(self, queue, queue_name):
        """Instances should not equal when auto_delete differs"""
        assert queue != Queue(queue_name, arguments=QueueArguments())

    def test_invalid_name(self, queue):
        """Invalid name type"""
        with pytest.raises(ValueError):
            queue.name = None

    def test_get_arguments_empty(self, queue):
        """Get arguments when arguments is None"""
        assert queue.arguments == {}

    def test_get_arguments(self, queue):
        """Get arguments when arguments is None"""
        queue.arguments = QueueArguments(max_priority=1)
        assert queue.arguments == {"x-max-priority": 1}


class TestQueueDefinitionManager:
    """QueueDefinitionmanager Class Test Cases"""

    def test_init(self):
        """Test if the class instantiates"""
        QueueDefinitionManager()

    def test_default_exchange_init(self, queue_manager):
        """Test if the class initializes with a default exchange binding"""
        assert "default" in queue_manager.queue_bindings

    def test_register_exchange(self, queue_manager, exchange):
        """Test registering a new exchange"""
        queue_manager.register_exchange(exchange)
        assert queue_manager.exchanges[exchange.name] == exchange
        assert exchange.name in queue_manager.queue_bindings

    def test_register_duplicate_name_exchange(self, queue_manager, exchange):
        """Register an exchange with the same name"""
        queue_manager.register_exchange(exchange)
        queue_manager.register_exchange(exchange)

    def test_register_duplicate_name_different_exchange(
        self, queue_manager, exchange, exchange_name
    ):
        """Register an exchange with the same name but different instance attributes"""
        queue_manager.register_exchange(exchange)
        ex = Exchange(exchange_name, passive=True)
        with pytest.raises(ValueError):
            queue_manager.register_exchange(ex)

    def test_register_queue(self, queue_manager, queue, exchange):
        """Test registering a new queue"""
        queue_manager.register_exchange(exchange)
        queue_manager.register_queue(queue, exchange_name=exchange.name)

    def test_register_duplicate_name_queue(self, queue_manager, queue, exchange):
        """Register a queue with the same name"""
        queue_manager.register_exchange(exchange)
        queue_manager.register_queue(queue, exchange.name)
        queue_manager.register_queue(queue, exchange.name)

    def test_register_duplicate_name_different_queue(
        self, queue_manager, queue, queue_name, exchange
    ):
        """Register a queue with the same name but different instance attributes"""
        queue_manager.register_exchange(exchange)
        queue_manager.register_queue(queue, exchange.name)
        qu = Queue(queue_name, passive=True)
        with pytest.raises(ValueError):
            queue_manager.register_queue(qu, exchange.name)

    def test_register_queue_to_exchange_no_routing_keys(
        self, queue_manager, queue, queue_name, exchange
    ):
        """Register a queue without routhing keys to an exchange"""
        exchange.exchange_type = ExchangeType.DIRECT
        queue_manager.register_exchange(exchange)
        with pytest.raises(AttributeError):
            queue_manager.register_queue(queue, exchange.name)

    def test_register_queue_default_exchange(self, queue_manager, queue):
        """Register a queue to the default exchange"""
        queue_manager.register_queue(queue)
        assert queue in queue_manager.queue_bindings[queue_manager.default_exchange]

    def test_register_queue_before_exchange(self, queue_manager, queue, exchange_name):
        """Register a queue to an exchange before the exchange is registered"""
        with pytest.raises(KeyError):
            queue_manager.register_queue(queue, exchange_name)

    def test_register_queue_multiple_exchange(self, queue_manager, queue, exchange):
        """Register a queue to multiple exchanges"""
        exchange1 = Exchange("test1", ExchangeType.FANOUT)
        exchange2 = Exchange("test2", ExchangeType.FANOUT)
        queue_manager.register_exchange([exchange1, exchange2])
        queue_manager.register_queue(queue, ["test1", "test2"])
        assert queue_manager.queue_bindings["test1"][-1].name == "test"
        assert queue_manager.queue_bindings["test2"][-1].name == "test"
