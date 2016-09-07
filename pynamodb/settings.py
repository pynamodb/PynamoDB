import imp
import os
from os import getenv
from botocore.vendored import requests
import constants

import logging

log = logging.getLogger(__name__)

OVERRIDE_SETTINGS_PATH = getenv('PYNAMO_CONFIG', '/etc/pynamodb/settings_override.py')

settings = {}

class RequestSessionWithHeaders(requests.Session):

    def __init__(self):
        super(RequestSessionWithHeaders, self).__init__()
        request_Session_headers = get_settings_value('REQUEST_SESSION_HEADERS')
        for header_key in request_Session_headers:
            self.headers.update({header_key: request_Session_headers[header_key]})

override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings= imp.load_source(OVERRIDE_SETTINGS_PATH, OVERRIDE_SETTINGS_PATH)
    log.info('Override settings for pynamo available {0}'.format(OVERRIDE_SETTINGS_PATH))
else:
    log.info('Override settings for pynamo not available {0}'.format(OVERRIDE_SETTINGS_PATH))
    log.info('Using Default settings value')

def get_settings_value(key):
    if hasattr(override_settings, key):
        return getattr(override_settings, key)

    if hasattr(constants, key):
        return getattr(constants, key)

    return None