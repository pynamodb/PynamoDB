"""
PynamoDB exceptions
"""

from typing import Any, Optional

import botocore.exceptions


class PynamoDBException(Exception):
    """
    A common exception class
    """
    def __init__(self, msg: Optional[str] = None, cause: Optional[Exception] = None) -> None:
        self.msg = msg
        self.cause = cause
        super(PynamoDBException, self).__init__(self.msg)

    @property
    def cause_response_code(self) -> Optional[str]:
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Code')

    @property
    def cause_response_message(self) -> Optional[str]:
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Message')


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
    def __init__(self, table_name: str) -> None:
        msg = "Table does not exist: `{}`".format(table_name)
        super(TableDoesNotExist, self).__init__(msg)


class TransactWriteError(PynamoDBException):
    """
    Raised when a TransactWrite operation fails
    """
    pass


class TransactGetError(PynamoDBException):
    """
    Raised when a TransactGet operation fails
    """
    pass


class InvalidStateError(PynamoDBException):
    """
    Raises when the internal state of an operation context is invalid
    """
    msg = "Operation in invalid state"


class AttributeDeserializationError(TypeError):
    """
    Raised when attribute type is invalid
    """
    def __init__(self, attr_name: str, attr_type: str):
        msg = "Cannot deserialize '{}' attribute from type: {}".format(attr_name, attr_type)
        super(AttributeDeserializationError, self).__init__(msg)


class AttributeNullError(ValueError):
    def __init__(self, attr_name: str) -> None:
        self.attr_path = attr_name

    def __str__(self):
        return f"Attribute '{self.attr_path}' cannot be None"

    def prepend_path(self, attr_name: str) -> None:
        self.attr_path = attr_name + '.' + self.attr_path


class VerboseClientError(botocore.exceptions.ClientError):
    def __init__(self, error_response: Any, operation_name: str, verbose_properties: Optional[Any] = None):
        """ Modify the message template to include the desired verbose properties """
        if not verbose_properties:
            verbose_properties = {}

        self.MSG_TEMPLATE = (
            'An error occurred ({{error_code}}) on request ({request_id}) '
            'on table ({table_name}) when calling the {{operation_name}} '
            'operation: {{error_message}}'
        ).format(request_id=verbose_properties.get('request_id'), table_name=verbose_properties.get('table_name'))

        super(VerboseClientError, self).__init__(error_response, operation_name)
