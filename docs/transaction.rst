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

Here's an example of using a context manager for a **TransactWrite** operation:

.. code-block:: python

    from pynamodb.connection import Connection
    from pynamodb.connection.transactions import TransactWrite

    # Two existing bank statements in the following states
    user1_statement = BankStatement('user1', account_balance=2000, is_active=True).save()
    user2_statement = BankStatement('user2', account_balance=0, is_active=True).save()

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

The **ConditionCheck** operation is used on a **TransactWrite** to check if the current state of a record you
aren't modifying within the overall transaction fits some criteria that, if it fails, would cause the entire
transaction to fail. The :`condition`:code: argument is of type `Condition <https://pynamodb.readthedocs.io/en/latest/conditional.html>`_.

========= ========
  field   required
========= ========
model_cls True
hash_key  True
range_key False
condition True

Delete
------

Save
----

Update
------


Transact Gets
^^^^^^^^^^^^^
.. code-block:: python

    with TransactGet(connection=connection) as transaction:
        """ attempting to get records of users' bank statements """
        user1_statement_promise = transaction.get(BankStatement, 'user1')
        user2_statement_promise = transaction.get(BankStatement, 'user2')

    user1_statement = user1_statement_promise.get()
    user2_statement = user2_statement_promise.get()

The **TransactGet** operation currently only supports the **Get** method, which only takes the following parameters:

========= ========
  field   required
========= ========
model_cls True
hash_key  True
range_key False

The `.get`

Error Types
^^^^^^^^^^^
