import threading
import time

import mock

from amqpstorm.exception import AMQPChannelError
from amqpstorm.rpc import Rpc
from amqpstorm.tests.utility import FakeConnection
from amqpstorm.tests.utility import FakePayload
from amqpstorm.tests.utility import TestFramework


class RpcTests(TestFramework):
    def test_rpc_register_request(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertEqual(len(rpc._request), 1)
        for key in rpc._request:
            self.assertEqual(uuid, rpc._request[key])

    def test_rpc_get_response_frame(self):
        rpc = Rpc(FakeConnection())
        rpc._response['travis-ci'] = ['travis-ci']
        self.assertEqual(rpc._get_response_frame('travis-ci'), 'travis-ci')

    def test_rpc_get_response_frame_empty(self):
        rpc = Rpc(FakeConnection())
        rpc._response['travis-ci'] = []
        self.assertIsNone(rpc._get_response_frame('travis-ci'))

    def test_rpc_get_request(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertTrue(rpc.on_frame(FakePayload(name='travis-ci')))
        self.assertIsInstance(rpc.get_request(uuid=uuid, raw=True),
                              FakePayload)

    @mock.patch('amqpstorm.rpc.Rpc._wait_for_request', return_value=False)
    def test_rpc_get_request_no_reply(self, _):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertIsNone(rpc.get_request(uuid=uuid, raw=False),
                          FakePayload)

    @mock.patch('amqpstorm.rpc.Rpc._wait_for_request', return_value=False)
    def test_rpc_get_request_no_reply_raw(self, _):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertIsNone(rpc.get_request(uuid=uuid, raw=True),
                          FakePayload)

    def test_rpc_get_request_timeout(self):
        rpc = Rpc(FakeConnection(), timeout=0.1)
        uuid = rpc.register_request(['travis-ci'])
        self.assertRaisesRegexp(
            AMQPChannelError,
            'rpc requests %s \(travis-ci\) took too long'
            % uuid,
            rpc.get_request, uuid=uuid, raw=False
        )

    def test_rpc_get_request_timeout_raw(self):
        rpc = Rpc(FakeConnection(), timeout=0.1)
        uuid = rpc.register_request(['travis-ci'])
        self.assertRaisesRegexp(
            AMQPChannelError,
            'rpc requests %s \(travis-ci\) took too long'
            % uuid,
            rpc.get_request, uuid=uuid, raw=False
        )

    def test_rpc_get_request_multiple_1(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        for index in range(1000):
            rpc.on_frame(FakePayload(name='travis-ci', value=index))
        for index in range(1000):
            result = rpc.get_request(uuid=uuid, raw=True, multiple=True)
            self.assertEqual(result.value, index)

        rpc.remove(uuid)

    def test_rpc_get_request_multiple_2(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        for index in range(1000):
            rpc.on_frame(FakePayload(name='travis-ci', value=index))
            result = rpc.get_request(uuid=uuid, raw=True, multiple=True)
            self.assertEqual(result.value, index)

        rpc.remove(uuid)

    def test_rpc_remove(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertEqual(len(rpc._request), 1)
        self.assertEqual(len(rpc._response), 1)
        self.assertEqual(len(rpc._response[uuid]), 0)
        rpc.on_frame(FakePayload(name='travis-ci'))
        rpc.remove(uuid)
        self.assertEqual(len(rpc._request), 0)
        self.assertEqual(len(rpc._response), 0)

    def test_rpc_remove_multiple(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        for index in range(1000):
            rpc.on_frame(FakePayload(name='travis-ci', value=index))
        self.assertEqual(len(rpc._request), 1)
        self.assertEqual(len(rpc._response[uuid]), 1000)
        rpc.remove(uuid)
        self.assertEqual(len(rpc._request), 0)
        self.assertEqual(len(rpc._response), 0)

    def test_rpc_remove_request(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertEqual(len(rpc._request), 1)
        rpc.remove_request(uuid)
        self.assertEqual(len(rpc._request), 0)

    def test_rpc_remove_response(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertEqual(len(rpc._response), 1)
        self.assertEqual(len(rpc._response[uuid]), 0)
        rpc.remove_response(uuid)
        self.assertEqual(len(rpc._response), 0)

    def test_rpc_remove_request_none(self):
        rpc = Rpc(FakeConnection())
        self.assertIsNone(rpc.remove_request(None))

    def test_rpc_remove_response_none(self):
        rpc = Rpc(FakeConnection())
        self.assertIsNone(rpc.remove_response(None))

    def test_rpc_get_request_not_found(self):
        rpc = Rpc(FakeConnection())
        self.assertIsNone(rpc.get_request(None))

    def test_rpc_on_frame(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertEqual(rpc._response[uuid], [])
        rpc.on_frame(FakePayload(name='travis-ci'))
        self.assertIsInstance(rpc._response[uuid][0], FakePayload)

    def test_rpc_on_multiple_frames(self):
        rpc = Rpc(FakeConnection())
        uuid = rpc.register_request(['travis-ci'])
        self.assertEqual(rpc._response[uuid], [])
        rpc.on_frame(FakePayload(name='travis-ci'))
        rpc.on_frame(FakePayload(name='travis-ci'))
        rpc.on_frame(FakePayload(name='travis-ci'))
        self.assertIsInstance(rpc._response[uuid][0], FakePayload)
        self.assertIsInstance(rpc._response[uuid][1], FakePayload)
        self.assertIsInstance(rpc._response[uuid][2], FakePayload)

    def test_rpc_raises_on_timeout(self):
        rpc = Rpc(FakeConnection(), timeout=0.01)
        rpc.register_request(['travis-ci-1'])
        uuid = rpc.register_request(['travis-ci-2'])
        rpc.register_request(['travis-ci-3'])
        self.assertEqual(rpc._response[uuid], [])
        time.sleep(0.1)
        self.assertRaisesRegexp(
            AMQPChannelError,
            'rpc requests %s \(travis-ci-2\) took too long' % uuid,
            rpc.get_request, uuid
        )

    def test_wait_for_request(self):
        rpc = Rpc(FakeConnection(), timeout=1)
        uuid = rpc.register_request(['travis-ci'])

        def delivery_payload():
            time.sleep(0.1)
            rpc.on_frame(FakePayload(name='travis-ci'))

        thread = threading.Thread(target=delivery_payload)
        thread.start()

        rpc._wait_for_request(
            uuid, connection_adapter=rpc._default_connection_adapter
        )

    def test_with_for_request_with_custom_adapter(self):
        class Adapter(object):
            def check_for_errors(self):
                pass

        rpc = Rpc(FakeConnection(), timeout=1)
        uuid = rpc.register_request(['travis-ci'])

        def delivery_payload():
            time.sleep(0.1)
            rpc.on_frame(FakePayload(name='travis-ci'))

        thread = threading.Thread(target=delivery_payload)
        thread.start()

        rpc._wait_for_request(uuid, Adapter())

    def test_with_for_request_with_custom_adapter_and_error(self):
        class Adapter(object):
            def check_for_errors(self):
                raise AMQPChannelError('travis-ci')

        rpc = Rpc(FakeConnection(), timeout=1)
        uuid = rpc.register_request(['travis-ci'])

        def delivery_payload():
            time.sleep(0.1)
            rpc.on_frame(FakePayload(name='travis-ci'))

        thread = threading.Thread(target=delivery_payload)
        thread.start()

        adapter = Adapter()
        self.assertRaises(
            AMQPChannelError,
            rpc._wait_for_request, uuid, adapter
        )
