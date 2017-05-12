import sys

import mock
import pytest

from pynamodb.connection import Connection
from pynamodb.connection.signals import _FakeNamespace
from pynamodb.connection.signals import pre_boto_send, post_boto_send

try:
    import blinker
except ImportError:
    blinker = None


pytestmark = pytest.mark.skipif(
    blinker is None,
    reason='Signals require the blinker library.'
)

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


@mock.patch(PATCH_METHOD)
@mock.patch('pynamodb.connection.base.uuid')
def test_signal(mock_uuid, mock_req):
    pre_recorded = []
    post_recorded = []
    UUID = '123-abc'

    def record_pre_boto_send(sender, operation_name, table_name, req_uuid):
        pre_recorded.append((operation_name, table_name, req_uuid))

    def record_post_boto_send(sender, operation_name, table_name, req_uuid):
        post_recorded.append((operation_name, table_name, req_uuid))

    pre_boto_send.connect(record_pre_boto_send)
    post_boto_send.connect(record_post_boto_send)
    try:
        mock_uuid.uuid4.return_value = UUID
        mock_req.return_value = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
        c = Connection()
        c.dispatch('CreateTable', {'TableName': 'MyTable'})
        assert ('CreateTable', 'MyTable', UUID) == pre_recorded[0]
        assert ('CreateTable', 'MyTable', UUID) == post_recorded[0]
    finally:
        pre_boto_send.disconnect(record_pre_boto_send)
        post_boto_send.disconnect(record_post_boto_send)


def test_fake_signals():
    with pytest.raises(RuntimeError):
        _signals = _FakeNamespace()

        pre_boto_send = _signals.signal('pre_boto_send')
        pre_boto_send.connect(lambda x: x)
