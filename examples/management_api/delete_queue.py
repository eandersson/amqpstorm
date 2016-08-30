from amqpstorm.management import ApiConnectionError
from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://127.0.0.1:15672', 'guest', 'guest')
    try:
        API.queue.delete('my_queue', virtual_host='/')
    except ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except ApiError as why:
        print('Failed to delete queue: %s' % why)
    else:
        print("Queue deleted...")
