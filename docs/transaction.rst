Transaction Operations
======================

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

    with TransactWrite(client_request_toke='super-unique-key') as transaction:
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


Transact Gets
^^^^^^^^^^^^^

Error Types
^^^^^^^^^^^
