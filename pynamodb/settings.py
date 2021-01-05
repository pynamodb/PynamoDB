import importlib.util
import logging
import os
import warnings
from os import getenv

from typing import Any, Optional, Mapping, ClassVar

log = logging.getLogger(__name__)

default_settings_dict = {
    'connect_timeout_seconds': 15,
    'read_timeout_seconds': 30,
    'max_retry_attempts': 3,
    'base_backoff_ms': 25,
    'region': 'us-east-1',
    'max_pool_connections': 10,
    'extra_headers': None,
}

OVERRIDE_SETTINGS_PATH = getenv('PYNAMODB_CONFIG', '/etc/pynamodb/global_default_settings.py')


def _load_module(name, path):
    # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore
    return module


override_settings = {}
if os.path.isfile(OVERRIDE_SETTINGS_PATH):
    override_settings = _load_module('__pynamodb_override_settings__', OVERRIDE_SETTINGS_PATH)
    if hasattr(override_settings, 'session_cls') or hasattr(override_settings, 'request_timeout_seconds'):
        warnings.warn("The `session_cls` and `request_timeout_second` options are no longer supported")
    log.info('Override settings for pynamo available {}'.format(OVERRIDE_SETTINGS_PATH))
else:
    log.info('Override settings for pynamo not available {}'.format(OVERRIDE_SETTINGS_PATH))
    log.info('Using Default settings value')


def get_settings_value(key: str) -> Any:
    """
    Fetches the value from the override file.
    If the value is not present, then tries to fetch the values from constants.py
    """
    if hasattr(override_settings, key):
        return getattr(override_settings, key)

    if key in default_settings_dict:
        return default_settings_dict[key]

    return None


class OperationSettings:
    """
    Settings applicable to an individual operation.
    When set, the settings in this object supersede the global and model settings.
    """
    default: ClassVar['OperationSettings']

    def __init__(self, *, extra_headers: Optional[Mapping[str, Optional[str]]] = None) -> None:
        """
        Initializes operation settings.
        :param extra_headers: if set, extra headers to add to the HTTP request. The headers are merged
         on top of extra headers derived from settings or models' Meta classes. To delete a header, set its value
         to `None`.
        """
        self.extra_headers = extra_headers


OperationSettings.default = OperationSettings()
