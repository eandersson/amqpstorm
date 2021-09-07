import uuid

from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://localhost:15672', 'guest', 'guest')
    API.user.create('my_user', str(uuid.uuid4()))

    # Get a user
    print(API.user.get('my_user'))

    # User that does not exist throws an exception
    API.user.delete('my_user')
    try:
        API.user.get('NOT_FOUND')
    except ApiError as why:
        if why.error_code == 404:
            print('User not found')
