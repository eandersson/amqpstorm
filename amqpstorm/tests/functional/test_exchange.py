import amqpstorm
from amqpstorm.tests.functional.utility import TestFunctionalFramework
from amqpstorm.tests.functional.utility import setup


class ExchangeFunctionalTests(TestFunctionalFramework):
    @setup(exchange=True)
    def test_functional_exchange_declare(self):
        self.channel.exchange.declare(self.exchange_name,
                                      passive=False,
                                      durable=True, auto_delete=True)
        self.assertEqual({}, self.channel.exchange.declare(self.exchange_name,
                                                           passive=True))

    @setup(exchange=True)
    def test_functional_exchange_delete(self):
        self.channel.exchange.declare(self.exchange_name)
        self.channel.exchange.delete(self.exchange_name,
                                     if_unused=True)
        self.assertRaises(amqpstorm.AMQPChannelError,
                          self.channel.exchange.declare,
                          self.exchange_name, passive=True)

    @setup(exchange=True, override_names=['exchange1', 'exchange2'])
    def test_functional_exchange_bind(self):
        self.channel.exchange.declare('exchange1')
        self.channel.exchange.declare('exchange2')

        self.assertEqual(self.channel.exchange.bind('exchange1',
                                                    'exchange2',
                                                    'routing_key'), {})

    @setup(exchange=True, override_names=['exchange1', 'exchange2'])
    def test_functional_exchange_unbind(self):
        self.channel.exchange.declare('exchange1')
        self.channel.exchange.declare('exchange2')
        self.channel.exchange.bind('exchange1', 'exchange2', 'routing_key')

        self.assertEqual(self.channel.exchange.unbind('exchange1', 'exchange2',
                                                      'routing_key'), {})
