"""
Mock response
"""
from urllib3 import HTTPResponse


class MockResponse(HTTPResponse):
    """
    A class for mocked responses
    """
    def __init__(self, status_code=None, content='Empty'):
        super(MockResponse, self).__init__()
        self.status_code = status_code
        self._content = content
        self.reason = 'Test Response'


class HttpOK(MockResponse):
    """
    A response that returns status code 200
    """
    def __init__(self, content=None):
        super(HttpOK, self).__init__(status_code=200, content=content)
