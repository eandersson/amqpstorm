import os

CURRENT_DIR = os.path.dirname(os.path.realpath(__file__))

HOST = os.environ.get('AMQP_HOST', '127.0.0.1')
USERNAME = os.environ.get('AMQP_USERNAME', 'guest')
PASSWORD = os.environ.get('AMQP_PASSWORD', 'guest')
URI = os.environ.get('AMQP_URI', 'amqp://guest:guest@127.0.0.1:5672/%2F')
HTTP_URL = os.environ.get('AMQP_HTTP_URL', 'http://127.0.0.1:15672')

SSL_URI = os.environ.get('AMQP_SSL_URI',
                         'amqps://guest:guest@rmq.eandersson.net:5671/%2F')
SSL_HOST = os.environ.get('AMQP_SSL_HOST', 'rmq.eandersson.net')
CAFILE = os.environ.get('AMQP_CAFILE',
                        '{0}/resources/cacert.pem'.format(CURRENT_DIR))
