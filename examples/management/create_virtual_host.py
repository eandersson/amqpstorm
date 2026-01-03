from amqpstorm import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.eandersson.net:15671', 'guest',
                                   'guest', verify=True)
    try:
        # Create a new Virtual Host called 'travis_ci'.
        API.virtual_host.create('travis_ci')
        print('Virtual Host created...')
    except management.ApiError as why:
        print('Failed to create virtual host: %s' % why)

    try:
        # Update the Virtual Host permissions for guest.
        API.user.set_permission('guest',
                                virtual_host='travis_ci',
                                configure_regex='.*',
                                write_regex='.*',
                                read_regex='.*')
        print('Permission updated created...')
    except management.ApiError as why:
        print('Failed to update permissions: %s' % why)
