from amqpstorm.management import ApiConnectionError
from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://localhost:15672', 'guest', 'guest')
    try:
        API.user.create('my_user', 'password')
        API.user.set_permission('my_user',
                                virtual_host='/',
                                configure_regex='.*',
                                write_regex='.*',
                                read_regex='.*')
    except ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except ApiError as why:
        print('Failed to create user: %s' % why)
    else:
        print("User created...")
