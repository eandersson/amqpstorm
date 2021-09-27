import json

import amqpstorm
import amqpstorm_pool

uri = 'amqp://guest:guest@rmq.amqpstorm.io:5672/%2F?heartbeat=60'
pool = amqpstorm_pool.QueuedPool(
    create=lambda: amqpstorm.UriConnection(uri),
    max_size=10,
    max_overflow=10,
    timeout=10,
    recycle=3600,
    stale=45,
)

with pool.acquire() as cxn:
    cxn.channel.queue.declare('game.matchmaking')
    cxn.channel.basic.publish(
        body=json.dumps({
            'type': 'matchmaking',
            'description': 'matchmaking message'
        }),
        exchange='',
        routing_key='game.matchmaking',
        properties={
            'content_type': 'text/plain',
            'headers': {'data': 'game'}
        }
    )
