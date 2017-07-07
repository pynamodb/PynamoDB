import mock
import pytest

from pynamodb.connection import Connection
from pynamodb.signals import _FakeNamespace
from pynamodb.signals import pre_dynamodb_send, post_dynamodb_send

try:
    import blinker
except ImportError:
    blinker = None

PATCH_METHOD = 'pynamodb.connection.Connection._make_api_call'


@mock.patch(PATCH_METHOD)
@mock.patch('pynamodb.connection.base.uuid')
def test_signal(mock_uuid, mock_req):
    pre_recorded = []
    post_recorded = []
    UUID = '123-abc'

    def record_pre_dynamodb_send(sender, operation_name, table_name, req_uuid):
        pre_recorded.append((operation_name, table_name, req_uuid))

    def record_post_dynamodb_send(sender, operation_name, table_name, req_uuid):
        post_recorded.append((operation_name, table_name, req_uuid))

    pre_dynamodb_send.connect(record_pre_dynamodb_send)
    post_dynamodb_send.connect(record_post_dynamodb_send)
    try:
        mock_uuid.uuid4.return_value = UUID
        mock_req.return_value = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
        c = Connection()
        c.dispatch('CreateTable', {'TableName': 'MyTable'})
        assert ('CreateTable', 'MyTable', UUID) == pre_recorded[0]
        assert ('CreateTable', 'MyTable', UUID) == post_recorded[0]
    finally:
        pre_dynamodb_send.disconnect(record_pre_dynamodb_send)
        post_dynamodb_send.disconnect(record_post_dynamodb_send)


@mock.patch(PATCH_METHOD)
@mock.patch('pynamodb.connection.base.uuid')
def test_signal_exception_pre_signal(mock_uuid, mock_req):
    post_recorded = []
    UUID = '123-abc'

    def record_pre_dynamodb_send(sender, operation_name, table_name, req_uuid):
        raise ValueError()

    def record_post_dynamodb_send(sender, operation_name, table_name, req_uuid):
        post_recorded.append((operation_name, table_name, req_uuid))

    pre_dynamodb_send.connect(record_pre_dynamodb_send)
    post_dynamodb_send.connect(record_post_dynamodb_send)
    try:
        mock_uuid.uuid4.return_value = UUID
        mock_req.return_value = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
        c = Connection()
        c.dispatch('CreateTable', {'TableName': 'MyTable'})
        assert ('CreateTable', 'MyTable', UUID) == post_recorded[0]
    finally:
        pre_dynamodb_send.disconnect(record_pre_dynamodb_send)
        post_dynamodb_send.disconnect(record_post_dynamodb_send)


@mock.patch(PATCH_METHOD)
@mock.patch('pynamodb.connection.base.uuid')
def test_signal_exception_post_signal(mock_uuid, mock_req):
    pre_recorded = []
    UUID = '123-abc'

    def record_pre_dynamodb_send(sender, operation_name, table_name, req_uuid):
        pre_recorded.append((operation_name, table_name, req_uuid))

    def record_post_dynamodb_send(sender, operation_name, table_name, req_uuid):
        raise ValueError()

    pre_dynamodb_send.connect(record_pre_dynamodb_send)
    post_dynamodb_send.connect(record_post_dynamodb_send)
    try:
        mock_uuid.uuid4.return_value = UUID
        mock_req.return_value = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
        c = Connection()
        c.dispatch('CreateTable', {'TableName': 'MyTable'})
        assert ('CreateTable', 'MyTable', UUID) == pre_recorded[0]
    finally:
        pre_dynamodb_send.disconnect(record_pre_dynamodb_send)
        post_dynamodb_send.disconnect(record_post_dynamodb_send)


def test_fake_signals():
    _signals = _FakeNamespace()
    pre_dynamodb_send = _signals.signal('pre_dynamodb_send')
    with pytest.raises(RuntimeError):
        pre_dynamodb_send.connect(lambda x: x)
    pre_dynamodb_send.send(object, operation_name="UPDATE", table_name="TEST", req_uuid="something")
