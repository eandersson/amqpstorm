"""
A simple RPC Server.
"""
import amqpstorm

from amqpstorm import Message


def fib(number):
    if number == 0:
        return 0
    elif number == 1:
        return 1
    else:
        return fib(number - 1) + fib(number - 2)


def on_request(message):
    number = int(message.body)

    print(" [.] fib(%s)" % (number,))

    response = str(fib(number))

    properties = {
        'correlation_id': message.correlation_id
    }

    response = Message.create(message.channel, response, properties)
    response.publish(message.reply_to)

    message.ack()


if __name__ == '__main__':
    CONNECTION = amqpstorm.Connection('localhost', 'guest', 'guest')
    CHANNEL = CONNECTION.channel()

    CHANNEL.queue.declare(queue='rpc_queue')
    CHANNEL.basic.qos(prefetch_count=1)
    CHANNEL.basic.consume(on_request, queue='rpc_queue')

    print(" [x] Awaiting RPC requests")
    CHANNEL.start_consuming()
