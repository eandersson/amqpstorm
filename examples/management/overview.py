from amqpstorm3 import management

if __name__ == '__main__':
    # If using a self-signed certificate, change verify=True to point at your CA bundle.
    # You can disable certificate verification for testing by passing in verify=False.
    API = management.ManagementApi('https://rmq.eandersson.net:15671', 'guest',
                                   'guest', verify=True)
    try:
        result = API.overview()
        print('%s: %s' % (result.get('product_name'), result.get('product_version')))
        print('Erlang Version: %s' % result.get('erlang_full_version'))
        print('Cluster Name: %s' % result.get('cluster_name'))
        print('Total Messages: %s' % result.get('queue_totals').get('messages'))
    except management.ApiConnectionError as why:
        print('Connection Error: %s' % why)
    except management.ApiError as why:
        print('ApiError: %s' % why)
