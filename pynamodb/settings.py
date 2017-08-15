import imp
import logging
import os
from os import getenv

from botocore.vendored import requests

log = logging.getLogger(__name__)

default_settings_dict = {
    'request_timeout_seconds': 60,
    'max_retry_attempts': 3,
    'base_backoff_ms': 25,
    'region': _determine_region(),
    'session_cls': requests.Session,
    'allow_rate_limited_scan_without_consumed_capacity': False,
}

OVERRIDE_SETTINGS_PATH = getenv('PYNAMODB_CONFIG', '/etc/pynamodb/global_default_settings.py')

override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings = imp.load_source('__pynamodb_override_settings__', OVERRIDE_SETTINGS_PATH)
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


def _determine_region():
    """
    Determine the current region by asking the EC2 instance, the boto session
    and the environment variable `AWS_DEFAULT_REGION`.
    
    In the worst case fallback to `us-east-1`
    """
    try:
        region = requests.get(
            'http://169.254.169.254/latest/dynamic/instance-identity/document',
            timeout=1
        ).json()['region']
    except:
        region = _os.environ.get('AWS_DEFAULT_REGION')

    if not region:
        region = 'us-east-1'

    return region
