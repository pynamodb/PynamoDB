import sys
from mock import patch

import pytest
from six.moves import reload_module

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
            reload_module(pynamodb.settings)
    assert len(warns) == 1
    assert 'options are no longer supported' in str(warns[0].message)
