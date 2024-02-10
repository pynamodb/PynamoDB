"""
PynamoDB exceptions
"""
import sys
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
if sys.version_info >= (3, 8):
    from typing import Literal
else:
    from typing_extensions import Literal

import botocore.exceptions


class PynamoDBException(Exception):
    """
    Base class for all PynamoDB exceptions.
    """

    msg: str

    def __init__(self, msg: Optional[str] = None, cause: Optional[Exception] = None) -> None:
        self.msg = msg if msg is not None else self.msg
        self.cause = cause
        super(PynamoDBException, self).__init__(self.msg)

    @property
    def cause_response_code(self) -> Optional[str]:
        """
        The DynamoDB response code such as:

        - ``ConditionalCheckFailedException``
        - ``ProvisionedThroughputExceededException``
        - ``TransactionCanceledException``

        Inspect this value to determine the cause of the error and handle it.
        """
        return getattr(self.cause, 'response', {}).get('Error', {}).get('Code')

    @property
    def cause_response_message(self) -> Optional[str]:
        """
        The human-readable description of the error returned by DynamoDB.
        """
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


@dataclass
class CancellationReason:
    """
    A reason for a transaction cancellation.
    
    For a list of possible cancellation reasons and their semantics,
    see `TransactGetItems`_ and `TransactWriteItems`_ in the AWS documentation.

    .. _TransactGetItems: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html
    .. _TransactWriteItems: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
    """
    code: str
    message: Optional[str] = None
    raw_item: Optional[Dict[str, Dict[str, Any]]] = None


class TransactWriteError(PynamoDBException):
    """
    Raised when a :class:`~pynamodb.transactions.TransactWrite` operation fails.
    """

    @property
    def cancellation_reasons(self) -> List[Optional[CancellationReason]]:
        """
        When :attr:`.cause_response_code` is ``TransactionCanceledException``, this property lists
        cancellation reasons in the same order as the transaction items (one-to-one).
        Items which were not part of the reason for cancellation would have :code:`None` as the value.

        For a list of possible cancellation reasons and their semantics,
        see `TransactWriteItems`_ in the AWS documentation.

        .. _TransactWriteItems: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html
        """
        if not isinstance(self.cause, VerboseClientError):
            return []
        return self.cause.cancellation_reasons


class TransactGetError(PynamoDBException):
    """
    Raised when a :class:`~pynamodb.transactions.TransactGet` operation fails.
    """
    @property
    def cancellation_reasons(self) -> List[Optional[CancellationReason]]:
        """
        When :attr:`.cause_response_code` is ``TransactionCanceledException``, this property lists
        cancellation reasons in the same order as the transaction items (one-to-one).
        Items which were not part of the reason for cancellation would have :code:`None` as the value.

        For a list of possible cancellation reasons and their semantics,
        see `TransactGetItems`_ in the AWS documentation.

        .. _TransactGetItems: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html
        """
        if not isinstance(self.cause, VerboseClientError):
            return []
        return self.cause.cancellation_reasons


class InvalidStateError(PynamoDBException):
    """
    Raises when the internal state of an operation context is invalid.
    """
    msg = "Operation in invalid state"


class AttributeDeserializationError(TypeError):
    """
    Raised when attribute type is invalid during deserialization.
    """
    def __init__(self, attr_name: str, attr_type: str):
        msg = "Cannot deserialize '{}' attribute from type: {}".format(attr_name, attr_type)
        super(AttributeDeserializationError, self).__init__(msg)


class AttributeNullError(ValueError):
    """
    Raised when an attribute which is not nullable (:code:`null=False`) is unset during serialization.
    """

    def __init__(self, attr_name: str) -> None:
        self.attr_path = attr_name

    def __str__(self):
        return f"Attribute '{self.attr_path}' cannot be None"

    def prepend_path(self, attr_name: str) -> None:
        self.attr_path = attr_name + '.' + self.attr_path


class VerboseClientError(botocore.exceptions.ClientError):
    def __init__(
        self,
        error_response: Dict[str, Any],
        operation_name: str,
        verbose_properties: Optional[Any] = None,
        *,
        cancellation_reasons: Iterable[Optional[CancellationReason]] = (),
    ) -> None:
        """
        Like ClientError, but with a verbose message.

        :param error_response: Error response in shape expected by ClientError.
        :param operation_name: The name of the operation that failed.
        :param verbose_properties: A dict of properties to include in the verbose message.
        :param cancellation_reasons: For `TransactionCanceledException` error code,
          a list of cancellation reasons in the same order as the transaction's items (one to one).
          For items which were not a reason for the transaction cancellation, :code:`None` will be the value.
        """
        if not verbose_properties:
            verbose_properties = {}

        self.MSG_TEMPLATE = (
            'An error occurred ({{error_code}}) on request ({request_id}) '
            'on table ({table_name}) when calling the {{operation_name}} '
            'operation: {{error_message}}'
        ).format(request_id=verbose_properties.get('request_id'), table_name=verbose_properties.get('table_name'))

        self.cancellation_reasons = list(cancellation_reasons)

        super(VerboseClientError, self).__init__(
            error_response,  # type:ignore[arg-type]  # in stubs: botocore.exceptions._ClientErrorResponseTypeDef
            operation_name,
        )
