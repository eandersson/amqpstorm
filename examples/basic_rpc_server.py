__author__ = 'eandersson'
"""
RPC Server example based on code from the official RabbitMQ Tutorial.
http://www.rabbitmq.com/tutorials/tutorial-six-python.html
"""
import amqpstorm


CONNECTION = amqpstorm.Connection('localhost', 'guest', 'guest')
CHANNEL = CONNECTION.channel()
CHANNEL.queue.declare(queue='rpc_queue')


def fib(number):
    if number == 0:
        return 0
    elif number == 1:
        return 1
    else:
        return fib(number - 1) + fib(number - 2)


def on_request(body, channel, header, properties):
    number = int(body)

    print(" [.] fib(%s)" % (number,))
    response = number

    channel.basic.publish(exchange='',
                          routing_key=properties['reply_to'],
                          properties={
                              'correlation_id': properties['correlation_id']
                          },
                          body=str(response))
    channel.basic.ack(delivery_tag=header['delivery_tag'])


CHANNEL.basic.qos(prefetch_count=1)
CHANNEL.basic.consume(on_request, queue='rpc_queue')

print(" [x] Awaiting RPC requests")
CHANNEL.start_consuming()