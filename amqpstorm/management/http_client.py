import requests.api
from requests.auth import HTTPBasicAuth

from amqpstorm.compatibility import urlparse
from amqpstorm.management.exception import ApiConnectionError
from amqpstorm.management.exception import ApiError


class HTTPClient(object):
    def __init__(self, api_url, username, password, verify, cert, timeout):
        self.session = requests.Session()
        self.session.verify = verify
        self.session.cert = cert
        self._auth = HTTPBasicAuth(username, password)
        self._base_url = api_url
        self._timeout = timeout

    def get(self, path, payload=None, headers=None):
        """HTTP GET operation.

        :param path: URI Path
        :param payload: HTTP Body
        :param headers: HTTP Headers

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :return: Response
        """
        return self._request('get', path, payload, headers)

    def list(self, path, name=None, page_size=None, use_regex=False):
        """List operation (e.g. queue list).

        :param path: URI Path
        :param name: Filter by name, for example queue name, exchange name etc
        :param use_regex: Enables regular expression for the param name
        :param page_size: Number of elements per page

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :return: Response
        """
        params = dict()
        if name is not None:
            params['name'] = name
        if use_regex:
            if isinstance(use_regex, bool):
                use_regex = str(use_regex)
            params['use_regex'] = use_regex.lower()

        if page_size is None:
            return self._request('get', path, params=params)

        results = list()
        params['page'] = 1
        params['page_size'] = page_size
        params['pagination'] = True
        first_result = self._request('get', path, params=params)
        num_pages = first_result['page_count']
        current_page = first_result.get('page', 1)
        results.extend(first_result['items'])

        while current_page < num_pages:
            params['page'] = current_page + 1
            next_result = self._request('get', path, params=params)
            current_page = next_result['page']
            num_pages = next_result['page_count']
            results.extend(next_result.get('items', []))

        return results

    def post(self, path, payload=None, headers=None):
        """HTTP POST operation.

        :param path: URI Path
        :param payload: HTTP Body
        :param headers: HTTP Headers

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :return: Response
        """
        return self._request('post', path, payload, headers)

    def delete(self, path, payload=None, headers=None):
        """HTTP DELETE operation.

        :param path: URI Path
        :param payload: HTTP Body
        :param headers: HTTP Headers

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :return: Response
        """
        return self._request('delete', path, payload, headers)

    def put(self, path, payload=None, headers=None):
        """HTTP PUT operation.

        :param path: URI Path
        :param payload: HTTP Body
        :param headers: HTTP Headers

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :return: Response
        """
        return self._request('put', path, payload, headers)

    def _request(self, method, path, payload=None, headers=None, params=None):
        """HTTP operation.

        :param method: Operation type (e.g. post)
        :param path: URI Path
        :param payload: HTTP Body
        :param headers: HTTP Headers
        :param params: HTTP Parameters

        :raises ApiError: Raises if the remote server encountered an error.
        :raises ApiConnectionError: Raises if there was a connectivity issue.

        :return: Response
        """
        url = urlparse.urljoin(self._base_url, 'api/%s' % path)
        headers = headers or {}
        headers['content-type'] = 'application/json'
        try:
            response = self.session.request(
                method, url,
                auth=self._auth,
                data=payload,
                headers=headers,
                timeout=self._timeout,
                params=params,
            )
        except requests.RequestException as why:
            raise ApiConnectionError(str(why))

        json_response = self._get_json_output(response)
        self._check_for_errors(response, json_response)
        return json_response

    @staticmethod
    def _get_json_output(response):
        """Get JSON output from the HTTP response.

        :param requests.Response response:

        :return: Json payload
        """
        try:
            content = response.json()
        except ValueError:
            content = None
        return content

    @staticmethod
    def _check_for_errors(response, json_response):
        """Check payload for errors.

        :param response: HTTP response
        :param json_response: Json response

        :raises ApiError: Raises if the remote server encountered an error.

        :return:
        """
        status_code = response.status_code
        try:
            response.raise_for_status()
        except requests.HTTPError as why:
            raise ApiError(str(why), reply_code=status_code)
        if isinstance(json_response, dict) and 'error' in json_response:
            raise ApiError(json_response['error'], reply_code=status_code)
