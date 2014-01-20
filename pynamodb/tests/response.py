"""
Mock response
"""
from requests.models import Response

class Response(Response):
    """
    A class for mocked responses
    """
    def __init__(self, status_code, content):
        self.status_code = status_code
        self._content = content
