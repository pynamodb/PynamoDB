Transaction Operations
======================

Transact operations are similar to Batch operations, with the key differences being that the writes support the
inclusion of condition checks, and they all must fail or succeed together.


Transaction operations are supported using context managers. Keep in mind that DynamoDB imposes limits on the number of
items that a single transaction can contain.


Suppose you have defined a BankStatement model, like in the example below.

.. code-block:: python

    from pynamodb.models import Model
    from pynamodb.attributes import BooleanAttribute, NumberAttribute, UnicodeAttribute

    class BankStatement(Model):
        class Meta:
            table_name = 'BankStatement'

    user_id = UnicodeAttribute(hash_key=True)
    account_balance = NumberAttribute(default=0)
    is_active = BooleanAttribute()


Transact Writes
^^^^^^^^^^^^^^^

A :py:class:`TransactWrite <pynamodb.transactions.TransactWrite>` can be initialized with the following parameters:

* ``connection`` (required) - the :py:class:`Connection <pynamodb.connection.base.Connection>` used to make the request (see :ref:`low-level`)
* ``client_request_token`` - an idempotency key for the request (see `ClientRequestToken <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken>`_ in the DynamoDB API reference)
* ``return_consumed_capacity`` - determines the level of detail about provisioned throughput consumption that is returned in the response (see `ReturnConsumedCapacity <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ReturnConsumedCapacity>`_ in the DynamoDB API reference)
* ``return_item_collection_metrics`` - determines whether item collection metrics are returned (see `ReturnItemCollectionMetrics <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ReturnItemCollectionMetrics>`_ in the DynamoDB API reference)

Here's an example of using a context manager for a :py:class:`TransactWrite <pynamodb.transactions.TransactWrite>` operation:

.. code-block:: python

    from pynamodb.connection import Connection
    from pynamodb.transactions import TransactWrite

    # Two existing bank statements in the following states
    user1_statement = BankStatement('user1', account_balance=2000, is_active=True)
    user2_statement = BankStatement('user2', account_balance=0, is_active=True)

    user1_statement.save()
    user2_statement.save()

    connection = Connection()

    with TransactWrite(connection=connection, client_request_token='super-unique-key') as transaction:
        # attempting to transfer funds from user1's account to user2's
        transfer_amount = 1000
        transaction.update(
            BankStatement(user_id='user1'),
            actions=[BankStatement.account_balance.add(transfer_amount * -1)],
            condition=(
                (BankStatement.account_balance >= transfer_amount) &
                (BankStatement.is_active == True)
            )
        )
        transaction.update(
            BankStatement(user_id='user2'),
            actions=[BankStatement.account_balance.add(transfer_amount)],
            condition=(BankStatement.is_active == True)
        )

    user1_statement.refresh()
    user2_statement.refresh()

    assert user1_statement.account_balance == 1000
    assert user2_statement.account_balance == 1000


Now, say you make another attempt to debit one of the accounts when they don't have enough money in the bank:

.. code-block:: python

    from pynamodb.exceptions import TransactWriteError

    assert user1_statement.account_balance == 1000
    assert user2_statement.account_balance == 1000

    try:
        with TransactWrite(connection=connection, client_request_token='another-super-unique-key') as transaction:
            # attempting to transfer funds from user1's account to user2's
            transfer_amount = 2000
            transaction.update(
                BankStatement(user_id='user1'),
                actions=[BankStatement.account_balance.add(transfer_amount * -1)],
                condition=(
                    (BankStatement.account_balance >= transfer_amount) &
                    (BankStatement.is_active == True)
                )
            )
            transaction.update(
                BankStatement(user_id='user2'),
                actions=[BankStatement.account_balance.add(transfer_amount)],
                condition=(BankStatement.is_active == True)
            )
    except TransactWriteError as e:
        # Because the condition check on the account balance failed,
        # the entire transaction should be cancelled
        assert e.cause_response_code == 'TransactionCanceledException'

        user1_statement.refresh()
        user2_statement.refresh()
        # and both models should be unchanged
        assert user1_statement.account_balance == 1000
        assert user2_statement.account_balance == 1000


Condition Check
---------------

The ``ConditionCheck`` operation is used on a :py:class:`TransactWrite <pynamodb.transactions.TransactWrite>` to check if the current state of a record you
aren't modifying within the overall transaction fits some criteria that, if it fails, would cause the entire
transaction to fail. The ``condition`` argument is of type :ref:`conditional_operations`.

* ``model_cls`` (required)
* ``hash_key``  (required)
* ``range_key`` (optional)
* ``condition`` (required) - of type :py:class:`Condition <pynamodb.expressions.condition.Condition>` (see :ref:`conditional_operations`)

.. code-block:: python

    with TransactWrite(connection=connection) as transaction:
        transaction.condition_check(BankStatement, 'user1', condition=(BankStatement.is_active == True))


Delete
------

The ``Delete`` operation functions similarly to ``Model.delete``.

* ``model`` (required)
* ``condition`` (optional) - of type :py:class:`Condition <pynamodb.expressions.condition.Condition>` (see :ref:`conditional_operations`)

.. code-block:: python

    statement = BankStatement.get('user1')

    with TransactWrite(connection=connection) as transaction:
        transaction.delete(statement, condition=(~BankStatement.is_active))



Save
----

The ``Put`` operation functions similarly to ``Model.save``.

* ``model`` (required)
* ``condition`` (optional) - of type :py:class:`Condition <pynamodb.expressions.condition.Condition>` (see :ref:`conditional_operations`)
* ``return_values`` (optional) - the values that should be returned if the condition fails ((see `Put ReturnValuesOnConditionCheckFailure`_ in the DynamoDB API reference)

.. code-block:: python

    statement = BankStatement(user_id='user3', account_balance=20, is_active=True)

    with TransactWrite(connection=connection) as transaction:
        transaction.save(statement, condition=(BankStatement.user_id.does_not_exist()))


Update
------

The ``Update`` operation functions similarly to ``Model.update``.

* ``model`` (required)
* ``actions`` (required) - a list of type :py:class:`Action <pynamodb.expressions.update.Action>` (see :ref:`updates`)
* ``condition`` (optional) - of type :py:class:`Condition <pynamodb.expressions.condition.Condition>` (see :ref:`conditional_operations`)
* ``return_values`` (optional) - the values that should be returned if the condition fails (see `Update ReturnValuesOnConditionCheckFailure`_ in the DynamoDB API reference)


.. code-block:: python

    user1_statement = BankStatement('user1')
    with TransactWrite(connection=connection) as transaction:
        transaction.update(
            user1_statement,
            actions=[BankStatement.account_balance.set(0), BankStatement.is_active.set(False)]
            condition=(BankStatement.user_id.exists())
        )


Transact Gets
^^^^^^^^^^^^^
.. code-block:: python

    with TransactGet(connection=connection) as transaction:
        """ attempting to get records of users' bank statements """
        user1_statement_future = transaction.get(BankStatement, 'user1')
        user2_statement_future = transaction.get(BankStatement, 'user2')

    user1_statement: BankStatement = user1_statement_future.get()
    user2_statement: BankStatement = user2_statement_future.get()

The :py:class:`TransactGet <pynamodb.transactions.TransactGet>` operation currently only supports the ``Get`` method, which only takes the following parameters:

* ``model_cls`` (required)
* ``hash_key``  (required)
* ``range_key`` (optional)

The ``.get`` returns a class of type ``_ModelFuture`` that acts as a placeholder for the record until the transaction completes.

To retrieve the resolved model, you say `model_future.get()`. Any attempt to access this model before the transaction is complete
will result in a :py:class:`InvalidStateError <pynamodb.exceptions.InvalidStateError>`.

Error Types
^^^^^^^^^^^

You can expect some new error types with transactions, such as:

* :py:exc:`TransactWriteError <pynamodb.exceptions.TransactWriteError>` - thrown when a :py:class:`TransactWrite <pynamodb.transactions.TransactWrite>` request returns a bad response (see the `TransactWriteItems Errors`_ section in the DynamoDB API reference).
* :py:exc:`TransactGetError <pynamodb.exceptions.TransactGetError>` - thrown when a :py:class:`TransactGet <pynamodb.transactions.TransactGet>` request returns a bad response (see the `TransactGetItems Errors`_ section in the DynamoDB API reference).
* :py:exc:`InvalidStateError <pynamodb.exceptions.InvalidStateError>` - thrown when an attempt is made to access data on a :py:class:`_ModelFuture <pynamodb.models._ModelFuture>` before the `TransactGet` request is completed.

.. _Update ReturnValuesOnConditionCheckFailure: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Update.html#DDB-Type-Update-ReturnValuesOnConditionCheckFailure>
.. _Put ReturnValuesOnConditionCheckFailure: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Put.html#DDB-Type-Put-ReturnValuesOnConditionCheckFailure
.. _TransactWriteItems Errors: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#API_TransactWriteItems_Errors
.. _TransactGetItems Errors: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactGetItems.html#API_TransactGetItems_Errors
