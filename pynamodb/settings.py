import imp
import logging
import os
from os import getenv

log = logging.getLogger(__name__)

default_settings_dict = {
    'REQUEST_TIMEOUT_SECONDS': 25,
    'MAX_RETRY_ATTEMPTS': 3,
    'BASE_BACKOFF_MS': 25,
    'REGION': 'us-east-1'
}

OVERRIDE_SETTINGS_PATH = getenv('PYNAMODB_CONFIG', '/etc/pynamodb/global_default_settings.py')

override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings = imp.load_source(OVERRIDE_SETTINGS_PATH, OVERRIDE_SETTINGS_PATH)
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
