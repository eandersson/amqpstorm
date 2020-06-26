from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://localhost:15672', 'guest', 'guest')
    try:
        # Create a new Virtual Host called 'travis_ci'.
        API.virtual_host.create('travis_ci')
        print("Virtual Host created...")
    except ApiError as why:
        print('Failed to create virtual host: %s' % why)

    try:
        # Update the Virtual Host permissions for guest.
        API.user.set_permission('guest',
                                virtual_host='travis_ci',
                                configure_regex='.*',
                                write_regex='.*',
                                read_regex='.*')
        print("Permission updated created...")
    except ApiError as why:
        print('Failed to update permissions: %s' % why)
