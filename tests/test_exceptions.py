from botocore.exceptions import ClientError

from pynamodb.exceptions import PutError, TransactError


def test_get_cause_response_code():
    error = PutError(
        cause=ClientError(
            error_response={
                'Error': {
                    'Code': 'hello'
                }
            },
            operation_name='test'
        )
    )
    assert error.cause_response_code == 'hello'


def test_get_cause_response_code__no_code():
    error = PutError()
    assert error.cause_response_code is None


def test_get_cause_response_message():
    error = PutError(
        cause=ClientError(
            error_response={
                'Error': {
                    'Message': 'hiya'
                }
            },
            operation_name='test'
        )
    )
    assert error.cause_response_message == 'hiya'


def test_get_cause_response_message__no_message():
    error = PutError()
    assert error.cause_response_message is None


def test_transact_error__get_reason_list_from_message():
    message = 'Transaction cancelled, please refer cancellation reasons for specific reasons [None, None, None, ConditionalCheckFailed]'
    assert TransactError._get_reason_list_from_message(message) == [None, None, None, 'ConditionalCheckFailed']


def test_transact_error__get_reason_list_from_message__no_match():
    assert TransactError._get_reason_list_from_message('nope') is None


def test_transact_error__parse_cancel_reason():
    error = TransactError(
        transact_items=[{'TableName': 'toot'}, {'TableName': 'boot'}],
        msg='TransactionCanceledException',
        cause=ClientError(
            error_response={
                'Error': {
                    'Code': 'TransactionCanceledException',
                    'Message': 'Transaction cancelled, please refer cancellation reasons for specific reasons [None, ConditionalCheckFailed]'
                }
            },
            operation_name='test'
        )
    )
    assert error.cancel_reasons == [({'TableName': 'toot'}, None), ({'TableName': 'boot'}, 'ConditionalCheckFailed')]


def test_transact_error__parse_cancel_reason__not_canceled():
    error = TransactError(
        transact_items=[],
        msg='NotCanceled',
        cause=ClientError(
            error_response={
                'Error': {
                    'Code': 'NotCanceled'
                }
            },
            operation_name='test'
        )
    )
    assert error.cancel_reasons is None
