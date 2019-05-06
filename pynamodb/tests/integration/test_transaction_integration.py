import os
from datetime import datetime

import config as cfg
import pytest

from pynamodb.exceptions import PutError, DoesNotExist

from pynamodb.attributes import NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute, BooleanAttribute
from pynamodb.connection.transactions import TransactGet, TransactWrite

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


def get_error_code(error):
    return error.cause.response['Error'].get('Code')


@pytest.mark.ddblocal
class TestTransaction:

    @classmethod
    def setup_class(cls):
        # must ensure that the connection's host is the same as the models'
        from pynamodb.connection.transactions import _CONNECTION
        _CONNECTION.host = DDB_URL

        for m in TEST_MODELS:
            if not m.exists():
                m.create_table(
                    read_capacity_units=10,
                    write_capacity_units=10,
                    wait=True
                )

    @classmethod
    def teardown_class(cls):
        [_m.delete_table() for _m in TEST_MODELS if _m.exists()]

    def test_transact_write__error__idempotent_parameter_mismatch(self, ddb_url):
        """
        The reason this fails, even when we don't explicitly pass a client token in, is because
        botocore generates one for us
        """
        transaction = TransactWrite()
        assert User.exists()
        print('transaction host', transaction._connection.host)
        print('ddb url', ddb_url)
        User(1).save(in_transaction=transaction)
        User(2).save(in_transaction=transaction)

        try:
            # committing the first time, then adding more info and committing again
            transaction.commit()
            User(3).save(in_transaction=transaction)
            transaction.commit()
            assert False, 'Failed to raise error'
        except PutError as e:
            assert get_error_code(e) == IDEMPOTENT_PARAMETER_MISMATCH

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
            assert get_error_code(e) == TRANSACTION_CANCELLED

    # def test_transact_write__error__transaction_in_progress(self):
    #     transact_get = TransactGet()
    #     User.get(1, in_transaction=transact_get)
    #     BankStatement.get(1, in_transaction=transact_get)
    #
    #     transact_write = TransactWrite()
    #     User(1).save(in_transaction=transact_write)
    #     BankStatement(1).save(in_transaction=transact_write)
    #
    #     try:
    #         # getting and writing to the same models at the same time
    #         transact_write.commit()
    #         transact_get.commit()
    #         assert False, 'Failed to raise error'
    #     except (GetError, PutError) as e:
    #         assert get_error_code(e) == TRANSACTION_IN_PROGRESS

    def test_transact_get(self):
        # making sure these entries exist, and with the expected info
        User(1).save()
        BankStatement(1).save()
        User(2).save()
        BankStatement(2, balance=100).save()

        # get users and statements we just created
        transaction = TransactGet()
        User.get(1, in_transaction=transaction)
        BankStatement.get(1, in_transaction=transaction)
        User.get(2, in_transaction=transaction)
        BankStatement.get(2, in_transaction=transaction)
        transaction.commit()

        # assign them to variables after commit
        user1 = transaction.from_results(User, 1)
        user2 = transaction.from_results(User, 2)
        statement1 = transaction.from_results(BankStatement, 1)
        statement2 = transaction.from_results(BankStatement, 2)

        assert user1.user_id == statement1.user_id == 1
        assert statement1.balance == 0
        assert user2.user_id == statement2.user_id == 2
        assert statement2.balance == 100

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

        # # TODO: investigate why this causes an internal server error
        # statement1.update(
        #     actions=[BankStatement.balance.set(50)],
        #     in_transaction=transact_write
        # )
        # statement2.update(
        #     actions=[BankStatement.balance.set(50)],  # TODO: investigate why this causes an internal server error
        #     condition=(BankStatement.balance >= 50),
        #     in_transaction=transact_write
        # )

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

    def test_transact_write__one_of_each(self):
        transaction = TransactWrite()
        User.condition_check(1, in_transaction=transaction, condition=(User.user_id.exists()))
        User(2).delete(in_transaction=transaction)
        # BankStatement(2).update(
        #     actions=[BankStatement.active.set(False)],
        #     condition=(BankStatement.active == True),
        #     in_transaction=transaction
        # )
        # print(transaction._operation_kwargs)
        LineItem(4, amount=100, currency='USD').save(condition=(LineItem.user_id.does_not_exist()))
        transaction.commit()

        # confirming transaction correct and successful
        assert User.get(1)
        try:
            User.get(2)
            assert False, 'Failed to delete model'
        except DoesNotExist:
            assert True
        # updated_statement = BankStatement.get(2)
        # assert not updated_statement.active
        new_line_item = next(LineItem.query(4, scan_index_forward=False, limit=1), None)
        assert new_line_item
        assert new_line_item.amount == 100
        assert new_line_item.currency == 'USD'

    def test_transact_write__different_regions(self):
        # creating a model in a table outside the region everyone else operates in
        DifferentRegion(entry_index=0).save()

        transaction = TransactGet()
        User.get(1, in_transaction=transaction)
        BankStatement.get(1, in_transaction=transaction)
        DifferentRegion.get(0, in_transaction=transaction)
        transaction.commit()
        user, statement, diff_region = transaction.get_results_in_order()
        assert isinstance(user, User)
        assert isinstance(statement, BankStatement)
        assert isinstance(diff_region, DifferentRegion)
