from unittest.mock import patch

import pytest
from importlib import reload

import pynamodb.settings


@pytest.mark.parametrize('settings_str', [
    "session_cls = object()",
    "request_timeout_seconds = 5",
])
def test_override_old_attributes(settings_str, tmpdir):
    custom_settings = tmpdir.join("pynamodb_settings.py")
    custom_settings.write(settings_str)

    with patch.dict('os.environ', {'PYNAMODB_CONFIG': str(custom_settings)}):
        with pytest.warns(UserWarning) as warns:
            reload(pynamodb.settings)
    assert len(warns) == 1
    assert 'options are no longer supported' in str(warns[0].message)


def test_default_settings():
    """Ensure that the default settings are what we expect. This is mainly done to catch
    any potentially breaking changes to default settings.
    """
    assert pynamodb.settings.default_settings_dict == {
        'connect_timeout_seconds': 15,
        'read_timeout_seconds': 30,
        'max_retry_attempts': 3,
        'region': None,
        'max_pool_connections': 10,
        'extra_headers': None,
        'retry_configuration': 'LEGACY'
    }
