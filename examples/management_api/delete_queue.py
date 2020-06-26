from amqpstorm.management import ApiConnectionError
from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://localhost:15672', 'guest', 'guest')
    try:
        API.queue.delete('my_queue', virtual_host='/')
    except ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except ApiError as why:
        print('Failed to delete queue: %s' % why)
    else:
        print("Queue deleted...")
