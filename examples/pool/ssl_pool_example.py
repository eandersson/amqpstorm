import json
import ssl

import amqpstorm
import amqpstorm_pool

context = ssl.create_default_context(cafile='ca_certificate.pem')
context.load_cert_chain(
    certfile='client_certificate.pem',
    keyfile='private_key.pem',
)
ssl_options = {
    'context': context,
    'server_hostname': 'rmq.amqpstorm.io'
}

uri = 'amqps://guest:guest@rmq.amqpstorm.io:5671/%2F?heartbeat=60'
pool = amqpstorm_pool.QueuedPool(
    create=lambda: amqpstorm.UriConnection(uri, ssl_options=ssl_options),
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
