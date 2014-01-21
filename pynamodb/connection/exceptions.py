"""
Pynamodb exceptions
"""


class PynamoDBException(Exception):
    """
    A common exception class
    """
    pass


class ConnectionError(PynamoDBException):
    """
    A base class for connection errors
    """

    msg = "Connection Error"

    def __init__(self, msg=None):
        super(ConnectionError, self).__init__(msg)


class DeleteError(ConnectionError):
    """
    Raised when an error occurs deleting an item
    """
    msg = "Error deleting item"


class QueryError(ConnectionError):
    """
    Raised when queries fail
    """
    msg = "Error performing query"


class ScanError(ConnectionError):
    """
    Raised when a scan operation fails
    """
    msg = "Error performing scan"


class PutError(ConnectionError):
    """
    Raised when an item fails to be created
    """
    msg = "Error putting item"


class UpdateError(ConnectionError):
    """
    Raised when an item fails to be updated
    """
    msg = "Error updating item"


class GetError(ConnectionError):
    """
    Raised when an item fails to be retrieved
    """
    msg = "Error getting item"


class TableError(ConnectionError):
    """
    An error involving a dynamodb table operation
    """
    msg = "Error performing a table operation"
