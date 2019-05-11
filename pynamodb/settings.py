import imp
import logging
import os
import warnings
from os import getenv

from botocore.vendored import requests

log = logging.getLogger(__name__)

default_settings_dict = {
    'connect_timeout_seconds': 15,
    'read_timeout_seconds': 30,
    'max_retry_attempts': 3,
    'base_backoff_ms': 25,
    'region': 'us-east-1',
    'max_pool_connections': 10,
    'extra_headers': None,
    'allow_rate_limited_scan_without_consumed_capacity': False,
}

OVERRIDE_SETTINGS_PATH = getenv('PYNAMODB_CONFIG', '/etc/pynamodb/global_default_settings.py')

override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings = imp.load_source('__pynamodb_override_settings__', OVERRIDE_SETTINGS_PATH)
    if hasattr(override_settings, 'session_cls') or hasattr(override_settings, 'request_timeout_seconds'):
        warnings.warn("The `session_cls` and `request_timeout_second` options are no longer supported")
    log.info('Override settings for pynamo available {0}'.format(OVERRIDE_SETTINGS_PATH))
else:
    log.info('Override settings for pynamo not available {0}'.format(OVERRIDE_SETTINGS_PATH))
    log.info('Using Default settings value')


def get_settings_value(key):
    """
    Fetches the value from the override file.
    If the value is not present, then tries to fetch the values from constants.py
    """
    if hasattr(override_settings, key):
        return getattr(override_settings, key)

    if key in default_settings_dict:
        return default_settings_dict[key]

    return None
