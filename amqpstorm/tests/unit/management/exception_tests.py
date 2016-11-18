from amqpstorm.management.exception import ApiError

from amqpstorm.tests.utility import TestFramework


class ApiExceptionTests(TestFramework):
    def test_api_exception_error_code_matching(self):
        exception = ApiError('travis-ci', reply_code=400)

        self.assertEqual(str(exception), 'travis-ci')

        self.assertEqual(exception.error_code, 400)

    def test_api_exception_unknown_error_code(self):
        exception = ApiError('travis-ci', reply_code=123)

        self.assertEqual(str(exception), 'travis-ci')
        self.assertEqual(exception.error_code, 123)

        self.assertFalse(exception.error_type)
        self.assertFalse(exception.documentation)

    def test_api_exception_no_error_code(self):
        exception = ApiError('travis-ci')

        self.assertEqual(str(exception), 'travis-ci')

        self.assertFalse(exception.error_type)
        self.assertFalse(exception.error_code)
        self.assertFalse(exception.documentation)
