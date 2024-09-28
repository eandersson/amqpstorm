from amqpstorm import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.eandersson.net:15671', 'guest',
                                   'guest', verify=True)
    try:
        API.queue.declare('my_queue', virtual_host='/')
        print('Queue created...')
    except management.ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except management.ApiError as why:
        print('Failed to create queue: %s' % why)
