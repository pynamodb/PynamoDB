"""
PynamoDB exceptions
"""


class PynamoDBException(Exception):
    """
    A common exception class
    """
    def __init__(self, msg=None):
        self.msg = msg or self.msg
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
