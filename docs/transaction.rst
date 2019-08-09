Transaction Operations
======================

Transact operations are similar to Batch operations, with the key differences being that the writes support the
inclusion of condition checks, and they all must fail or succeed together.


Transaction operations are supported using context managers. Keep in mind that DynamoDB imposes limits on the number of
items that a single transaction can contain. PynamoDB doesn't currently handle automatic grouping or paginating, so this
is something you must handle on your own.


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

A `TransactWrite`:code: can be initialized with the following parameters:

* `connection`:code: (required) - the `Connection <https://pynamodb.readthedocs.io/en/latest/api.html#pynamodb.connection.Connection>`_ used to make the request
* `client_request_token`:code: - an idempotency key for the request (`see here <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ClientRequestToken>`_)
* `return_consumed_capacity`:code: - determines the level of detail about provisioned throughput consumption that is returned in the response (`see here <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ReturnConsumedCapacity>`_)
* `return_item_collection_metrics`:code: - determines whether item collection metrics are returned (`see here <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-request-ReturnItemCollectionMetrics>`_)

Here's an example of using a context manager for a `TransactWrite`:code: operation:

.. code-block:: python

    from pynamodb.connection import Connection
    from pynamodb.connection.transactions import TransactWrite

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

The `ConditionCheck`:code: operation is used on a `TransactWrite`:code: to check if the current state of a record you
aren't modifying within the overall transaction fits some criteria that, if it fails, would cause the entire
transaction to fail. The `condition`:code: argument is of type `Condition <https://pynamodb.readthedocs.io/en/latest/conditional.html>`_.

* `model_cls`:code: (required)
* `hash_key`:code:  (required)
* `range_key`:code: (optional)
* `condition`:code: (required) - of type `Condition <https://pynamodb.readthedocs.io/en/latest/conditional.html>`_

.. code-block:: python

    with TransactWrite(connection=connection) as transaction:
        transaction.condition_check(BankStatement, 'user1', condition=(BankStatement.is_active == True))


Delete
------

The `Delete`:code: operation functions similarly to `Model.delete`:code:.

* `model`:code: (required)
* `condition`:code: (optional) - of type `Condition <https://pynamodb.readthedocs.io/en/latest/conditional.html>`_

.. code-block:: python

    statement = BankStatement.get('user1')

    with TransactWrite(connection=connection) as transaction:
        transaction.delete(statement, condition=(~BankStatement.is_active))



Save
----

The `Put`:code: operation functions similarly to `Model.save`:code:.

* `model`:code: (required)
* `condition`:code: (optional) - of type `Condition <https://pynamodb.readthedocs.io/en/latest/conditional.html>`_
* `return_values`:code: (optional) - the values that should be returned if the condition fails (`see here <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Put.html#DDB-Type-Put-ReturnValuesOnConditionCheckFailure>`_)

.. code-block:: python

    statement = BankStatement(user_id='user3', account_balance=20, is_active=True)

    with TransactWrite(connection=connection) as transaction:
        transaction.save(statement, condition=(BankStatement.user_id.does_not_exist()))


Update
------

The `Update`:code: operation functions similarly to `Model.update`:code:.

* `model_cls`:code: (required)
* `hash_key`:code:  (required)
* `range_key`:code: (optional)
* `actions`:code: (required) - a list of type `Action <https://pynamodb.readthedocs.io/en/latest/updates.html>`_
* `condition`:code: (optional) - of type `Condition <https://pynamodb.readthedocs.io/en/latest/conditional.html>`_
* `return_values`:code: (optional) - the values that should be returned if the condition fails (`see here <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Update.html#DDB-Type-Update-ReturnValuesOnConditionCheckFailure>`_)


.. code-block:: python

    with TransactWrite(connection=connection) as transaction:
        transaction.update(
            BankStatement,
            'user1',
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

The `TransactGet`:code: operation currently only supports the `Get`:code: method, which only takes the following parameters:

* `model_cls`:code: (required)
* `hash_key`:code:  (required)
* `range_key`:code: (optional)

The `.get`:code: returns a class of type `_ModelFuture`:code: that acts as a placeholder for the record until the transaction completes.

To retrieve the resolved model, you say `model_future.get()`. Any attempt to access this model before the transaction is complete
will result in a `InvalidStateError`:code:.

Error Types
^^^^^^^^^^^

You can expect some new error types with transactions, such as:

* `TransactWriteError`:code: - thrown when a `TransactWrite`:code: request returns a bad response.
* `TransactGetError`:code: - thrown when a `TransactGet`:code: request returns a bad response.
* `InvalidStateError`:code: - thrown when an attempt is made to access data on a `_ModelFuture`:code: before the `TransactGet` request is completed.

You can learn more about the new error messages `here <https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_TransactWriteItems.html#DDB-TransactWriteItems-response-ItemCollectionMetrics>`_
