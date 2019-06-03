import os
from datetime import datetime

import config as cfg
import pytest

from pynamodb.exceptions import PutError, DoesNotExist

from pynamodb.attributes import NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute, BooleanAttribute
from pynamodb.connection.transactions import TransactGet, TransactWrite, Transaction

from pynamodb.models import Model

IDEMPOTENT_PARAMETER_MISMATCH = 'IdempotentParameterMismatchException'
PROVISIONED_THROUGHPUT_EXCEEDED = 'ProvisionedThroughputExceededException'
RESOURCE_NOT_FOUND = 'ResourceNotFoundException'
TRANSACTION_CANCELLED = 'TransactionCanceledException'
TRANSACTION_IN_PROGRESS = 'TransactionInProgressException'


DDB_URL = os.getenv('PYNAMODB_INTEGRATION_TEST_DDB_URL', cfg.DYNAMODB_HOST)


class User(Model):
    class Meta:
        host = DDB_URL
        region = 'us-east-1'
        table_name = 'user'

    user_id = NumberAttribute(hash_key=True)


class BankStatement(Model):

    class Meta:
        host = DDB_URL
        region = 'us-east-1'
        table_name = 'statement'

    user_id = NumberAttribute(hash_key=True)
    balance = NumberAttribute(default=0)
    active = BooleanAttribute(default=True)


class LineItem(Model):

    class Meta:
        host = DDB_URL
        region = 'us-east-1'
        table_name = 'line-item'

    user_id = NumberAttribute(hash_key=True)
    created_at = UTCDateTimeAttribute(range_key=True, default=datetime.now())
    amount = NumberAttribute()
    currency = UnicodeAttribute()


class DifferentRegion(Model):

    class Meta:
        host = DDB_URL
        region = 'us-east-2'
        table_name = 'different-region'

    entry_index = NumberAttribute(hash_key=True)


TEST_MODELS = [
    BankStatement,
    DifferentRegion,
    LineItem,
    User,
]


class TestTransactionIntegration:

    @classmethod
    def setup_class(cls):
        # ensure that the transaction connection url is what we expect
        Transaction(host=DDB_URL, override_connection=True)
        for m in TEST_MODELS:
            if not m.exists():
                m.create_table(
                    read_capacity_units=10,
                    write_capacity_units=10,
                    wait=True
                )

    @classmethod
    def teardown_class(cls):
        for m in TEST_MODELS:
            if m.exists():
                m.delete_table()

    @staticmethod
    def get_error_code(error):
        return error.cause.response['Error'].get('Code')

    @pytest.mark.ddblocal
    def test_transact_write__error__idempotent_parameter_mismatch(self):

        transaction = TransactWrite(host=DDB_URL)
        User(1).save(in_transaction=transaction)
        User(2).save(in_transaction=transaction)

        try:
            # committing the first time, then adding more info and committing again
            transaction.commit()
            User(3).save(in_transaction=transaction)
            transaction.commit()
            assert False, 'Failed to raise error'
        except PutError as e:
            assert self.get_error_code(e) == IDEMPOTENT_PARAMETER_MISMATCH

    @pytest.mark.ddblocal
    def test_transact_write__error__different_regions(self):
        # creating a model in a table outside the region everyone else operates in
        transact_write = TransactWrite()
        DifferentRegion(entry_index=0).save(in_transaction=transact_write)
        BankStatement(1).save(in_transaction=transact_write)
        User(1).save(in_transaction=transact_write)

        try:
            transact_write.commit()
        except PutError as e:
            assert self.get_error_code(e) == RESOURCE_NOT_FOUND

    @pytest.mark.ddblocal
    def test_transact_write__error__transaction_cancelled(self):
        # create a users and a bank statements for them
        User(1).save()
        BankStatement(1).save()

        # attempt to do this as a transaction with the condition that they don't already exist
        transaction = TransactWrite()
        User(1).save(condition=(User.user_id.does_not_exist()), in_transaction=transaction)
        BankStatement(1).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transaction)
        try:
            transaction.commit()
            assert False, 'Failed to raise error'
        except PutError as e:
            assert self.get_error_code(e) == TRANSACTION_CANCELLED

    @pytest.mark.ddblocal
    def test_transact_get(self):
        # making sure these entries exist, and with the expected info
        User(1).save()
        BankStatement(1).save()
        User(2).save()
        BankStatement(2, balance=100).save()

        # get users and statements we just created and assign them to variables
        transaction = TransactGet()
        user1 = User.get(1, in_transaction=transaction)
        statement1 = BankStatement.get(1, in_transaction=transaction)
        user2 = User.get(2, in_transaction=transaction)
        statement2 = BankStatement.get(2, in_transaction=transaction)
        transaction.commit()

        assert user1.user_id == statement1.user_id == 1
        assert statement1.balance == 0
        assert user2.user_id == statement2.user_id == 2
        assert statement2.balance == 100

    @pytest.mark.ddblocal
    def test_transact_write(self):
        # making sure these entries exist, and with the expected info
        BankStatement(1, balance=0).save()
        BankStatement(2, balance=100).save()

        # assert values are what we think they should be
        statement1 = BankStatement.get(1)
        statement2 = BankStatement.get(2)
        assert statement1.balance == 0
        assert statement2.balance == 100

        # let the users send money to one another
        created_at = datetime.now()
        transaction = TransactWrite()
        # create a credit line item to user 1's account
        LineItem(user_id=1, amount=50, currency='USD').save(
            condition=(LineItem.user_id.does_not_exist()),
            in_transaction=transaction
        )
        # create a debit to user 2's account
        LineItem(user_id=2, amount=-50, currency='USD').save(
            condition=(LineItem.user_id.does_not_exist()),
            in_transaction=transaction
        )

        # add credit to user 1's account
        statement1.balance += 50
        statement1.save(in_transaction=transaction)
        # debit from user 2's account if they have enough in the bank
        statement2.balance -= 50
        statement2.save(condition=(BankStatement.balance >= 50), in_transaction=transaction)
        transaction.commit()

        statement1.refresh()
        statement2.refresh()
        assert statement1.balance == statement2.balance == 50

    @pytest.mark.ddblocal
    def test_transact_write__one_of_each(self):
        transaction = TransactWrite()
        User.condition_check(1, in_transaction=transaction, condition=(User.user_id.exists()))
        User(2).delete(in_transaction=transaction)
        LineItem(4, amount=100, currency='USD').save(condition=(LineItem.user_id.does_not_exist()))
        transaction.commit()

        # confirming transaction correct and successful
        assert User.get(1)
        try:
            User.get(2)
            assert False, 'Failed to delete model'
        except DoesNotExist:
            assert True

        new_line_item = next(LineItem.query(4, scan_index_forward=False, limit=1), None)
        assert new_line_item
        assert new_line_item.amount == 100
        assert new_line_item.currency == 'USD'
