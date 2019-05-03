from datetime import datetime

import config as cfg
import pytest
from pynamodb.exceptions import GetError

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
BankStatement(2).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transact_write)
transact_write.commit()

# get users and statements we just created and assign them to variables
transact_get = TransactGet(host=cfg.DYNAMODB_HOST, region='us-east-1')
user1 = User.get(1, in_transaction=transact_get)
statement1 = BankStatement.get(1, in_transaction=transact_get)
user2 = User.get(1, in_transaction=transact_get)
statement2 = BankStatement.get(1, in_transaction=transact_get)

# test we can't access user information before commit
with pytest.raises(GetError):
    gotten_id = user1.user_id

transact_get.commit()

assert user1.user_id == 1
assert statement1.user_id == 1
assert statement1.balance == 0

assert user2.user_id == 2
assert statement2.user_id == 2
assert statement2.balance == 0



for model in TEST_MODELS:
    if model.exists():
        print("Deleting table for model {0}".format(model.Meta.table_name))
        model.delete_table()
