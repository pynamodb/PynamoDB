"""
PynamoDB exceptions
"""

import botocore.exceptions


class PynamoDBException(Exception):
    """
    A common exception class
    """
    def __init__(self, msg=None, cause=None):
        self.msg = msg or self.msg
        self.cause = cause
        super(PynamoDBException, self).__init__(self.msg)


class PynamoDBConnectionError(PynamoDBException):
    """
    A base class for connection errors
    """
    msg = "Connection Error"


class DeleteError(PynamoDBConnectionError):
    """
    Raised when an error occurs deleting an item
    """
    msg = "Error deleting item"


class QueryError(PynamoDBConnectionError):
    """
    Raised when queries fail
    """
    msg = "Error performing query"


class ScanError(PynamoDBConnectionError):
    """
    Raised when a scan operation fails
    """
    msg = "Error performing scan"


class PutError(PynamoDBConnectionError):
    """
    Raised when an item fails to be created
    """
    msg = "Error putting item"


class UpdateError(PynamoDBConnectionError):
    """
    Raised when an item fails to be updated
    """
    msg = "Error updating item"


class GetError(PynamoDBConnectionError):
    """
    Raised when an item fails to be retrieved
    """
    msg = "Error getting item"


class TableError(PynamoDBConnectionError):
    """
    An error involving a dynamodb table operation
    """
    msg = "Error performing a table operation"


class DoesNotExist(PynamoDBException):
    """
    Raised when an item queried does not exist
    """
    msg = "Item does not exist"


class TableDoesNotExist(PynamoDBException):
    """
    Raised when an operation is attempted on a table that doesn't exist
    """
    def __init__(self, table_name):
        msg = "Table does not exist: `{0}`".format(table_name)
        super(TableDoesNotExist, self).__init__(msg)


class VerboseClientError(botocore.exceptions.ClientError):
    def __init__(self, error_response, operation_name, verbose_properties=None):
        """ Modify the message template to include the desired verbose properties """
        if not verbose_properties:
            verbose_properties = {}

        self.MSG_TEMPLATE = (
            'An error occurred ({{error_code}}) on request ({request_id}) '
            'on table ({table_name}) when calling the {{operation_name}} '
            'operation: {{error_message}}'
        ).format(request_id=verbose_properties.get('request_id'), table_name=verbose_properties.get('table_name'))

        super(VerboseClientError, self).__init__(error_response, operation_name)

