Transaction Operations
======================

Transact operations are similar to Batch operations, with the key differences being that the writes support the
inclusion of condition checks, and they all must fail or succeed together.


Transact operations are supported using context managers. The DynamoDB API has limits on the number of items in
each request, but PynamoDB doesn't currently handle grouping or paginating, so this is something you must handle on your
own.

.. note::

    An ordered array of up to 25 `TransactWriteItem` objects, each of which contains a `ConditionCheck`, `Put`, `Update`, or `Delete` object. These can operate on items in different tables, but the tables must reside in the same AWS account and Region, and no two of them can operate on the same item.

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

Here's an example of using a context manager for a `TransactWrite` operation:

.. code-block:: python

    connection = Connection()

    with TransactWrite(connection=connection, client_request_toke='super-unique-key') as transaction:
        """ attempting to transfer funds from user1's account to user2's """
        transfer_amount = 1000
        transaction.update(
            BankStatement(user_id='user1'),
            actions=[BankStatement.account_balance.add(transfer_amount * -1)],
            condition=(
                (BankStatement.account_balance >= transfer_amount) &
                (BankStatement.is_active)
            )
        )
        transaction.update(
            BankStatement(user_id='user2'),
            actions=[BankStatement.account_balance.add(transfer_amount)],
            condition=(BankStatement.is_active)
        )

Condition Check
---------------

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

The `TransactGet` operation currently only supports the `Get` method, which only takes the following parameters:

- model_cls (required)
- hash_key (required)
- range_key (optional)

The `.get`

Error Types
^^^^^^^^^^^
