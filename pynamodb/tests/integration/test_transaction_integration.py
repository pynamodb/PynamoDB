from datetime import datetime

import config as cfg

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
    created_at = UTCDateTimeAttribute(default=datetime.now)
    amount = NumberAttribute()
    currency = UnicodeAttribute()


TEST_MODELS = [User, BankStatement, LineItem]

for model in TEST_MODELS:
    if not model.exists():
        print("Creating table for model {0}".format(model.Meta.table_name))
        model.create_table(read_capacity_units=1, write_capacity_units=1, wait=True)

# create a users and a bank statements for them if neither already exist
transact_write = TransactWrite(host=cfg.DYNAMODB_HOST, region='us-east-1')
User(1).save(condition=(User.user_id.does_not_exist()), in_transaction=transact_write)
BankStatement(1).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transact_write)
User(2).save(condition=(User.user_id.does_not_exist()), in_transaction=transact_write)
BankStatement(2, balance=100).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transact_write)
transact_write.commit()

# get users and statements we just created
transact_get = TransactGet(host=cfg.DYNAMODB_HOST, region='us-east-1')
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

# let the users send money to one another, reusing connection from earlier
created_at = datetime.now()
transact_write = TransactWrite()
# create a credit line item to user 1's account
LineItem(user_id=1, amount=50, currency='USD', created_at=created_at).save(
    condition=(LineItem.created_at.does_not_exist()),
    in_transaction=transact_write
)
# create a debit to user 2's account
LineItem(user_id=2, amount=-50, currency='USD', created_at=created_at).save(
    condition=(LineItem.created_at.does_not_exist()),
    in_transaction=transact_write
)
# add credit to user 1's account
statement1.update(
    actions=[BankStatement.balance.add(50)],
    in_transaction=transact_write
)
# debit from user 2's account if they have enough in the bank
# add credit to user 1's account
statement2.update(
    actions=[BankStatement.balance.add(-50)],
    condition=(BankStatement.balance >= 50),
    in_transaction=transact_write
)
transact_write.commit()

statement1.refresh()
statement2.refresh()

assert statement1.balance == 50
assert statement2.balance == 50

for model in TEST_MODELS:
    if model.exists():
        print("Deleting table for model {0}".format(model.Meta.table_name))
        model.delete_table()
