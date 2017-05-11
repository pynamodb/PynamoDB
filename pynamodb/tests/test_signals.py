import json

import mock
import pytest
import requests

from pynamodb.connection import Connection
from pynamodb.connection.signals import pre_boto_send, post_boto_send

try:
    import blinker
except ImportError:
    blinker = None

pytestmark = pytest.mark.skipif(
    blinker is None,
    reason='Signals require the blinker library.'
)

@mock.patch('pynamodb.connection.Connection.session')
@mock.patch('pynamodb.connection.Connection.requests_session')
@mock.patch('pynamodb.connection.base.uuid')
def test_signal(mock_uuid, requests_session_mock, session_mock):
    pre_recorded = []
    post_recorded = []
    UUID = '123-abc'

    def record_pre_boto_send(sender, operation_name, table_name, req_uuid):
        pre_recorded.append((operation_name, table_name, req_uuid))

    def record_post_boto_send(sender, operation_name, table_name, req_uuid):
        post_recorded.append((operation_name, table_name, req_uuid))

    good_response_content = {'TableDescription': {'TableName': 'table', 'TableStatus': 'Creating'}}
    good_response = requests.Response()
    good_response.status_code = 200
    good_response._content = json.dumps(good_response_content).encode('utf-8')

    pre_boto_send.connect(record_pre_boto_send)
    post_boto_send.connect(record_post_boto_send)
    try:
        mock_uuid.uuid4.return_value = UUID
        requests_session_mock.send.return_value = good_response
        c = Connection()
        c._make_api_call('CreateTable', {'TableName': 'MyTable'})
        assert ('CreateTable', 'MyTable', UUID) == pre_recorded[0]
        assert ('CreateTable', 'MyTable', UUID) == post_recorded[0]
    finally:
        pre_boto_send.disconnect(record_pre_boto_send)
        post_boto_send.disconnect(record_post_boto_send)
