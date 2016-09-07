import imp
import os
from os import getenv
import constants

import logging

log = logging.getLogger(__name__)

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

    if hasattr(constants, key):
        return getattr(constants, key)

    return None