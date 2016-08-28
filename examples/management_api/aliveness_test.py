from amqpstorm.management import ApiConnectionError
from amqpstorm.management import ApiError
from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://192.168.1.100:15672', 'guest', 'guest')
    try:
        result = API.aliveness_test('/')
        if result['status'] == 'ok':
            print("RabbitMQ is alive!")
        else:
            print("RabbitMQ is not alive! :(")
    except ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except ApiError as why:
