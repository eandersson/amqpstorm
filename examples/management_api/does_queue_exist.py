from amqpstorm.management import ApiConnectionError
from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://localhost:15672', 'guest', 'guest')
    try:
        API.queue.declare('my_queue', virtual_host='/', passive=True)
    except ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except ApiError as why:
        if why.error_code != 404:
            raise
        print('Queue does not exist')
    else:
        print("Queue does exist...")
