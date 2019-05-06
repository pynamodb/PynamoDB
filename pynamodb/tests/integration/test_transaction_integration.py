from datetime import datetime

import config as cfg
import pytest

from pynamodb.exceptions import PutError, GetError

from pynamodb.attributes import NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute
from pynamodb.connection.transaction import TransactGet, TransactWrite

from pynamodb.models import Model

IDEMPOTENT_PARAMETER_MISMATCH = 'IdempotentParameterMismatchException'
PROVISIONED_THROUGHPUT_EXCEEDED = 'ProvisionedThroughputExceededException'
RESOURCE_NOT_FOUND = 'ResourceNotFoundException'
TRANSACTION_CANCELLED = 'TransactionCanceledException'
TRANSACTION_IN_PROGRESS = 'TransactionInProgressException'


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


class DifferentRegion(Model):

    class Meta:
        region = 'us-east-2'
        table_name = 'different-region'
        host = cfg.DYNAMODB_HOST

    entry_index = NumberAttribute(hash_key=True)


TEST_MODELS = [
    BankStatement,
    DifferentRegion,
    LineItem,
    User,
]


@pytest.fixture(scope='class')
def setup(request):
    # must ensure that the connection's host is the same as the models'
    from pynamodb.connection.transaction import _CONNECTION
    _CONNECTION.host = cfg.DYNAMODB_HOST

    for m in TEST_MODELS:
        if not m.exists():
            m.create_table(
                read_capacity_units=10,
                write_capacity_units=10,
                wait=True
            )

    def fin():
        [_m.delete_table() for _m in TEST_MODELS if _m.exists()]

    request.addfinalizer(finalizer=fin)


def get_error_code(error):
    return error.cause.response['Error'].get('Code')


@pytest.mark.usefixtures('setup')
@pytest.mark.ddblocal
class TestTransaction:

    def test_transact_write__error__idempotent_parameter_mismatch(self):
        # even when we don't pass a client_request_token, the transaction generates its own
        # to ensure idempotency
        transact_write = TransactWrite()
        User(1).save(in_transaction=transact_write)
        User(2).save(in_transaction=transact_write)

        try:
            # committing the first time, then adding more info and committing again
            transact_write.commit()
            User(3).save(in_transaction=transact_write)
            transact_write.commit()
            assert False, 'Failed to raise error'
        except PutError as e:
            assert get_error_code(e) == IDEMPOTENT_PARAMETER_MISMATCH

    def test_transact_write__error__transaction_cancelled(self):
        # create a users and a bank statements for them
        User(1).save()
        BankStatement(1).save()

        # attempt to do this as a transaction with the condition that they don't already exist
        transact_write1 = TransactWrite()
        User(1).save(condition=(User.user_id.does_not_exist()), in_transaction=transact_write1)
        BankStatement(1).save(condition=(BankStatement.user_id.does_not_exist()), in_transaction=transact_write1)
        try:
            transact_write1.commit()
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

    def test_transact_write(self):
        # making sure these entries exist, and with the expected info
        BankStatement(1, balance=0).save()
        BankStatement(2, balance=100).save()
        transact_get = TransactGet()
        BankStatement.get(1, in_transaction=transact_get)
        BankStatement.get(2, in_transaction=transact_get)
        transact_get.commit()

        # assert values are what we think they should be
        statement1, statement2 = transact_get.get_results_in_order()
        assert statement1.balance == 0
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
        statement1.balance += 50
        statement1.save(in_transaction=transact_write)
        # debit from user 2's account if they have enough in the bank
        statement2.balance -= 50
        statement2.save(condition=(BankStatement.balance >= 50), in_transaction=transact_write)
        transact_write.commit()

        statement1.refresh()
        statement2.refresh()
        assert statement1.balance == 50
        assert statement2.balance == 50

    def test_transact_write__update_existing(self):
        # # TODO: investigate why this (update) causes an internal server error
        # statement1.update(
        #     actions=[BankStatement.balance.set(50)],
        #     in_transaction=transact_write
        # )
        # statement2.update(
        #     actions=[BankStatement.balance.set(50)],  # TODO: investigate why this causes an internal server error
        #     condition=(BankStatement.balance >= 50),
        #     in_transaction=transact_write
        # )
        pass

    def test_transact_write__different_regions(self):
        # creating a model in a table outside the region everyone else operates in
        DifferentRegion(entry_index=0).save()

        transact_get = TransactGet()
        User.get(1, in_transaction=transact_get)
        BankStatement.get(1, in_transaction=transact_get)
        DifferentRegion.get(0, in_transaction=transact_get)
        transact_get.commit()
        user, statement, diff_region = transact_get.get_results_in_order()
        assert isinstance(user, User)
        assert isinstance(statement, BankStatement)
        assert isinstance(diff_region, DifferentRegion)
