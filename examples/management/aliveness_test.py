from amqpstorm import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.amqpstorm.io:15671', 'guest',
                                   'guest', verify=True)
    try:
        result = API.aliveness_test('/')
        if result['status'] == 'ok':
            print('RabbitMQ is alive!')
        else:
            print('RabbitMQ is not alive! :(')
    except management.ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except management.ApiError as why:
        print('ApiError: %s' % why)
