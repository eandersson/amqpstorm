from amqpstorm import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.amqpstorm.io:15671', 'guest',
                                   'guest', verify=True)
    API.user.create('my_user', 'password')

    # Get a user
    print(API.user.get('my_user'))

    # User that does not exist throws an exception
    API.user.delete('my_user')
    try:
        API.user.get('NOT_FOUND')
    except management.ApiError as why:
        if why.error_code == 404:
            print('User not found')
