import os

HOST = os.environ.get('AMQP_HOST', '127.0.0.1')
USERNAME = os.environ.get('AMQP_USERNAME', 'guest')
PASSWORD = os.environ.get('AMQP_PASSWORD', 'guest')
URI = os.environ.get('AMQP_URI', 'amqp://guest:guest@127.0.0.1:5672/%2F')
HTTP_URL = os.environ.get('AMQP_HTTP_URL', 'http://127.0.0.1:15672')
