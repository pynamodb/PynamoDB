from os import getenv
from pynamodb.constants import DEFAULT_REGION
import imp
import os
import logging

log = logging.getLogger(__name__)

default_settings_dict = {}
default_settings_dict['REQUEST_TIMEOUT_SECONDS'] = 25
default_settings_dict['MAX_RETRY_ATTEMPTS'] = 3
default_settings_dict['BASE_BACKOFF_MS'] = 25
default_settings_dict['REGION'] = DEFAULT_REGION

OVERRIDE_SETTINGS_PATH = getenv('PYNAMO_CONFIG', '/etc/pynamodb/settings_override.py')

override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings= imp.load_source(OVERRIDE_SETTINGS_PATH, OVERRIDE_SETTINGS_PATH)
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
