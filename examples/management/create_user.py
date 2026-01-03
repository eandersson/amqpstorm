from amqpstorm3 import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.eandersson.net:15671', 'guest',
                                   'guest', verify=True)
    try:
        API.user.create('my_user', 'password', tags='administrator')
        API.user.set_permission('my_user',
                                virtual_host='/',
                                configure_regex='.*',
                                write_regex='.*',
                                read_regex='.*')
        print('User Created')
    except management.ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except management.ApiError as why:
        print('Failed to create user: %s' % why)
