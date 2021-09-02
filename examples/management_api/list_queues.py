from amqpstorm.management import ManagementApi

if __name__ == '__main__':
    API = ManagementApi('http://localhost:15672', 'guest', 'guest')

    print('List all queues.')
    for queue in API.queue.list():
        print('%s: %s' % (queue.get('name'), queue.get('messages')))
    print('')

    print('List all queues containing the keyword: amqpstorm.')
    for queue in API.queue.list(name='amqpstorm'):
        print('%s: %s' % (queue.get('name'), queue.get('messages')))
    print('')

    print('List all queues using regex that starts with: amqpstorm.')
    for queue in API.queue.list(name='^amqpstorm', use_regex=True):
        print('%s: %s' % (queue.get('name'), queue.get('messages')))
