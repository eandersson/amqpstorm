from amqpstorm import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.eandersson.net:15671', 'guest',
                                   'guest', verify=True)
    try:
        result = API.whoami()
        print('I am: %s' % result.get('name'))
        print('I have the following tags: %s' % ','.join(result.get('tags')))
    except management.ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except management.ApiError as why:
        print('ApiError: %s' % why)
