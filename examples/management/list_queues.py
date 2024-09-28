from amqpstorm import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.eandersson.net:15671', 'guest',
                                   'guest', verify=True)

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
