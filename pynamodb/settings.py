import os.path
from botocore.vendored import requests

OVERRIDE_SETTINGS_FILE = '/etc/pynamodb/settings_override.py'

settings = {}

DEFAULT_REQUEST_SESSION_HEADERS = dict()
DEFAULT_REQUEST_SESSION_HEADERS['x-envoy-retry-on'] = '5xx,connect-failure'
DEFAULT_REQUEST_SESSION_HEADERS['x-envoy-max-retries'] = 3

default_settings_dict = {}
default_settings_dict['REQUEST_TIMEOUT_SECONDS'] = 25
default_settings_dict['MAX_RETRY_ATTEMPTS_EXCEPTION'] = 3
default_settings_dict['BASE_BACKOFF_MS'] = 25
default_settings_dict['REQUEST_SESSION_HEADERS'] = DEFAULT_REQUEST_SESSION_HEADERS

class RequestSessionWithHeaders(requests.Session):

    def __init__(self):
        super(RequestSessionWithHeaders, self).__init__()
        request_Session_headers = settings['request_session_headers']
        for header_key in request_Session_headers:
            self.headers.update({header_key: request_Session_headers[header_key]})

override_setting_dict = {}
if os.path.isfile(OVERRIDE_SETTINGS_FILE):
    with open(OVERRIDE_SETTINGS_FILE) as f:
        exec(f.read(), override_setting_dict)

def get_settings_value(key):
    if key in override_setting_dict:
        return override_setting_dict[key]

    return default_settings_dict[key]

settings['request_timeout_seconds'] = get_settings_value('REQUEST_TIMEOUT_SECONDS')
settings['max_retry_attempts'] = get_settings_value('MAX_RETRY_ATTEMPTS_EXCEPTION')
settings['base_backoff_ms'] = get_settings_value('BASE_BACKOFF_MS')
settings['request_session_headers'] = get_settings_value('REQUEST_SESSION_HEADERS')
settings['request_session_cls'] = RequestSessionWithHeaders