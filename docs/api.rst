API
===

High Level API
--------------

.. automodule:: pynamodb.models
    :members: Model

.. automodule:: pynamodb.attributes
    :members:

.. automodule:: pynamodb.indexes
    :members:

.. automodule:: pynamodb.transactions
    :members:

.. automodule:: pynamodb.pagination
    :members:

Low Level API
-------------

.. automodule:: pynamodb.connection
    :members: Connection, TableConnection

Exceptions
----------

.. autoexception:: pynamodb.exceptions.PynamoDBConnectionError
.. autoexception:: pynamodb.exceptions.DeleteError
.. autoexception:: pynamodb.exceptions.QueryError
.. autoexception:: pynamodb.exceptions.ScanError
.. autoexception:: pynamodb.exceptions.PutError
.. autoexception:: pynamodb.exceptions.UpdateError
.. autoexception:: pynamodb.exceptions.GetError
.. autoexception:: pynamodb.exceptions.TableError
.. autoexception:: pynamodb.exceptions.TableDoesNotExist
.. autoexception:: pynamodb.exceptions.DoesNotExist
.. autoexception:: pynamodb.exceptions.TransactWriteError
.. autoexception:: pynamodb.exceptions.TransactGetError
.. autoexception:: pynamodb.exceptions.InvalidStateError
.. autoexception:: pynamodb.exceptions.AttributeDeserializationError
.. autoexception:: pynamodb.exceptions.AttributeNullError
.. autoclass:: pynamodb.exceptions.CancellationReason
