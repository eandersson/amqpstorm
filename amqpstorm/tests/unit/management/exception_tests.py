try:
    import unittest2 as unittest
except ImportError:
    import unittest

from amqpstorm.management.exception import ApiError


class ApiExceptionTests(unittest.TestCase):
    def test_api_exception_error_code_matching(self):
        exception = ApiError('error', reply_code=400)

        self.assertEqual(str(exception), 'error')

        self.assertEqual(exception.error_code, 400)

    def test_api_exception_invalid_error_code(self):
        exception = ApiError('error', reply_code=123)

        self.assertEqual(str(exception), 'error')
        self.assertEqual(exception.error_code, 123)

        self.assertFalse(exception.error_type)
        self.assertFalse(exception.documentation)

    def test_api_exception_no_error_code(self):
        exception = ApiError('error')

        self.assertEqual(str(exception), 'error')

        self.assertFalse(exception.error_type)
        self.assertFalse(exception.error_code)
        self.assertFalse(exception.documentation)
