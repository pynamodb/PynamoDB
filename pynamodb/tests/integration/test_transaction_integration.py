from datetime import datetime

import config as cfg
from pynamodb.exceptions import PutError

from pynamodb.attributes import NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.connection.transaction import TransactGet, TransactWrite

from pynamodb.models import Model


class User(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'user'
        host = cfg.DYNAMODB_HOST

    user_id = NumberAttribute(hash_key=True)


class BankStatement(Model):

    class Meta:
        region = 'us-east-1'
        table_name = 'statement'
        host = cfg.DYNAMODB_HOST

    user_id = NumberAttribute(hash_key=True)
    balance = NumberAttribute(default=0)


class LineItem(Model):

    class Meta:
        region = 'us-east-1'
        table_name = 'line-item'
        host = cfg.DYNAMODB_HOST

    user_id = NumberAttribute(hash_key=True)
    created_at = UTCDateTimeAttribute(range_key=True, default=datetime.now())
    amount = NumberAttribute()
    currency = UnicodeAttribute()


TEST_MODELS = [User, BankStatement, LineItem]


def get_error_code(e):
    return e.cause.response['Error'].get('Code')


for model in TEST_MODELS:
    if not model.exists():
        print("Creating table for model {0}".format(model.Meta.table_name))
        model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

# create a users and a bank statements for them
User(1).save()
BankStatement(1).save()
User(2).save()
BankStatement(2, balance=100).save()

# attempt to do this as a transaction with the condition that they don't already exist
transact_write1 = TransactWrite(host=cfg.DYNAMODB_HOST, region='us-east-1')
User(1).save(condition=(User.user_id.does_not_exist()), in_transaction=transact_write1)
BankStatement(1).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transact_write1)
User(2).save(condition=(User.user_id.does_not_exist()), in_transaction=transact_write1)
BankStatement(2, balance=100).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transact_write1)
try:
    transact_write1.commit()
except PutError as e:
    assert get_error_code(e) == 'TransactionCanceledException'

# get users and statements we just created
transact_get = TransactGet()
User.get(1, in_transaction=transact_get)
BankStatement.get(1, in_transaction=transact_get)
User.get(2, in_transaction=transact_get)
BankStatement.get(2, in_transaction=transact_get)
transact_get.commit()

# assign them to variables after commit
user1 = transact_get.from_results(User, 1)
user2 = transact_get.from_results(User, 2)
statement1 = transact_get.from_results(BankStatement, 1)
statement2 = transact_get.from_results(BankStatement, 2)

assert user1.user_id == 1
assert statement1.user_id == 1
assert statement1.balance == 0
assert user2.user_id == 2
assert statement2.user_id == 2
assert statement2.balance == 100

# let the users send money to one another
created_at = datetime.now()
transact_write = TransactWrite()
# create a credit line item to user 1's account
LineItem(user_id=1, amount=50, currency='USD').save(
    condition=(LineItem.user_id.does_not_exist()),
    in_transaction=transact_write
)
# create a debit to user 2's account
LineItem(user_id=2, amount=-50, currency='USD').save(
    condition=(LineItem.user_id.does_not_exist()),
    in_transaction=transact_write
)
# add credit to user 1's account
'''
# TODO: investigate why this causes an internal server error
statement1.update(
    actions=[BankStatement.balance.set(50)],
    in_transaction=transact_write
)
statement2.update(
    actions=[BankStatement.balance.set(50)],  # TODO: investigate why this causes an internal server error
    condition=(BankStatement.balance >= 50),
    in_transaction=transact_write
)
'''
statement1.balance += 50
statement1.save(in_transaction=transact_write)
# debit from user 2's account if they have enough in the bank
# add credit to user 1's account
statement2.balance -= 50
statement2.save(condition=(BankStatement.balance >= 50), in_transaction=transact_write)
transact_write.commit()

statement1.refresh()
statement2.refresh()

assert statement1.balance == 50
assert statement2.balance == 50

for model in TEST_MODELS:
    if model.exists():
        print("Deleting table for model {0}".format(model.Meta.table_name))
        model.delete_table()
