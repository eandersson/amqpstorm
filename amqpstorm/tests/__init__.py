import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

HOST = os.environ.get(
    'AMQP_HOST',
    'rmq.eandersson.net'
)
USERNAME = os.environ.get(
    'AMQP_USERNAME',
    'amqpstorm'
)
PASSWORD = os.environ.get(
    'AMQP_PASSWORD',
    '2a55f70a841f18b'
)
URI = os.environ.get(
    'AMQP_URI',
    'amqp://{0}:{1}@rmq.eandersson.net:5672/%2F'.format(USERNAME, PASSWORD)
)
HTTP_URL = os.environ.get(
    'AMQP_HTTP_URL',
    'http://rmq.eandersson.net:15672'
)
HTTPS_URL = os.environ.get(
    'AMQP_HTTP_URL',
    'https://rmq.eandersson.net:15671'
)
SSL_URI = os.environ.get(
    'AMQP_SSL_URI',
    'amqps://{0}:{1}@rmq.eandersson.net:5671/%2F'.format(USERNAME, PASSWORD)
)
SSL_HOST = os.environ.get(
    'AMQP_SSL_HOST',
    'rmq.eandersson.net'
)
CAFILE = os.environ.get(
    'AMQP_CAFILE',
    '{0}/resources/ssl/ca_certificate.pem'.format(CURRENT_DIR)
)
CERTFILE = os.environ.get(
    'AMQP_CERTFILE',
    '{0}/resources/ssl/client/client_certificate.pem'.format(CURRENT_DIR)
)
KEYFILE = os.environ.get(
    'AMQP_KEYFILE',
    '{0}/resources/ssl/client/private_key.pem'.format(CURRENT_DIR)
)
