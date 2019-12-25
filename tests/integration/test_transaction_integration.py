import uuid
from datetime import datetime

import pytest

from pynamodb.connection import Connection
from pynamodb.exceptions import DoesNotExist, TransactWriteError, TransactGetError, InvalidStateError


from pynamodb.attributes import (
    NumberAttribute, UnicodeAttribute, UTCDateTimeAttribute, BooleanAttribute, VersionAttribute
)
from pynamodb.transactions import TransactGet, TransactWrite

from pynamodb.models import Model

IDEMPOTENT_PARAMETER_MISMATCH = 'IdempotentParameterMismatchException'
PROVISIONED_THROUGHPUT_EXCEEDED = 'ProvisionedThroughputExceededException'
RESOURCE_NOT_FOUND = 'ResourceNotFoundException'
TRANSACTION_CANCELLED = 'TransactionCanceledException'
TRANSACTION_IN_PROGRESS = 'TransactionInProgressException'
VALIDATION_EXCEPTION = 'ValidationException'


class User(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'user'

    user_id = NumberAttribute(hash_key=True)


class BankStatement(Model):

    class Meta:
        region = 'us-east-1'
        table_name = 'statement'

    user_id = NumberAttribute(hash_key=True)
    balance = NumberAttribute(default=0)
    active = BooleanAttribute(default=True)


class LineItem(Model):

    class Meta:
        region = 'us-east-1'
        table_name = 'line-item'

    user_id = NumberAttribute(hash_key=True)
    created_at = UTCDateTimeAttribute(range_key=True, default=datetime.now())
    amount = NumberAttribute()
    currency = UnicodeAttribute()


class DifferentRegion(Model):

    class Meta:
        region = 'us-east-2'
        table_name = 'different-region'

    entry_index = NumberAttribute(hash_key=True)


class Foo(Model):
    class Meta:
        region = 'us-east-1'
        table_name = 'foo'

    bar = NumberAttribute(hash_key=True)
    star = UnicodeAttribute(null=True)
    version = VersionAttribute()


TEST_MODELS = [
    BankStatement,
    DifferentRegion,
    LineItem,
    User,
    Foo
]


@pytest.fixture(scope='module')
def connection(ddb_url):
    yield Connection(host=ddb_url)


@pytest.fixture(scope='module', autouse=True)
def create_tables(ddb_url):
    for m in TEST_MODELS:
        m.Meta.host = ddb_url
        m.create_table(
            read_capacity_units=10,
            write_capacity_units=10,
            wait=True
        )

    yield

    for m in TEST_MODELS:
        if m.exists():
            m.delete_table()


def get_error_code(error):
    return error.cause.response['Error'].get('Code')


def get_error_message(error):
    return error.cause.response['Error'].get('Message')


@pytest.mark.ddblocal
def test_transact_write__error__idempotent_parameter_mismatch(connection):
    client_token = str(uuid.uuid4())

    with TransactWrite(connection=connection, client_request_token=client_token) as transaction:
        transaction.save(User(1))
        transaction.save(User(2))

    with pytest.raises(TransactWriteError) as exc_info:
        # committing the first time, then adding more info and committing again
        with TransactWrite(connection=connection, client_request_token=client_token) as transaction:
            transaction.save(User(3))
    assert get_error_code(exc_info.value) == IDEMPOTENT_PARAMETER_MISMATCH

    # ensure that the first request succeeded in creating new users
    assert User.get(1)
    assert User.get(2)

    with pytest.raises(DoesNotExist):
        # ensure it did not create the user from second request
        User.get(3)


@pytest.mark.ddblocal
def test_transact_write__error__different_regions(connection):
    with pytest.raises(TransactWriteError) as exc_info:
        with TransactWrite(connection=connection) as transact_write:
            # creating a model in a table outside the region everyone else operates in
            transact_write.save(DifferentRegion(entry_index=0))
            transact_write.save(BankStatement(1))
            transact_write.save(User(1))
    assert get_error_code(exc_info.value) == RESOURCE_NOT_FOUND


@pytest.mark.ddblocal
def test_transact_write__error__transaction_cancelled__condition_check_failure(connection):
    # create a users and a bank statements for them
    User(1).save()
    BankStatement(1).save()

    # attempt to do this as a transaction with the condition that they don't already exist
    with pytest.raises(TransactWriteError) as exc_info:
        with TransactWrite(connection=connection) as transaction:
            transaction.save(User(1), condition=(User.user_id.does_not_exist()))
            transaction.save(BankStatement(1), condition=(BankStatement.user_id.does_not_exist()))
    assert get_error_code(exc_info.value) == TRANSACTION_CANCELLED
    assert 'ConditionalCheckFailed' in get_error_message(exc_info.value)


@pytest.mark.ddblocal
def test_transact_write__error__multiple_operations_on_same_record(connection):
    BankStatement(1).save()

    # attempt to do a transaction with multiple operations on the same record
    with pytest.raises(TransactWriteError) as exc_info:
        with TransactWrite(connection=connection) as transaction:
            transaction.condition_check(BankStatement, 1, condition=(BankStatement.user_id.exists()))
            transaction.update(BankStatement(1), actions=[(BankStatement.balance.add(10))])
    assert get_error_code(exc_info.value) == VALIDATION_EXCEPTION


@pytest.mark.ddblocal
def test_transact_get(connection):
    # making sure these entries exist, and with the expected info
    User(1).save()
    BankStatement(1).save()
    User(2).save()
    BankStatement(2, balance=100).save()

    # get users and statements we just created and assign them to variables
    with TransactGet(connection=connection) as transaction:
        _user1_future = transaction.get(User, 1)
        _statement1_future = transaction.get(BankStatement, 1)
        _user2_future = transaction.get(User, 2)
        _statement2_future = transaction.get(BankStatement, 2)

    user1 = _user1_future.get()
    statement1 = _statement1_future.get()
    user2 = _user2_future.get()
    statement2 = _statement2_future.get()

    assert user1.user_id == statement1.user_id == 1
    assert statement1.balance == 0
    assert user2.user_id == statement2.user_id == 2
    assert statement2.balance == 100


@pytest.mark.ddblocal
def test_transact_get__does_not_exist(connection):
    with TransactGet(connection=connection) as transaction:
        _user_future = transaction.get(User, 100)
    with pytest.raises(User.DoesNotExist):
        _user_future.get()


@pytest.mark.ddblocal
def test_transact_get__invalid_state(connection):
    with TransactGet(connection=connection) as transaction:
        _user_future = transaction.get(User, 100)
        with pytest.raises(InvalidStateError):
            _user_future.get()


@pytest.mark.ddblocal
def test_transact_write(connection):
    # making sure these entries exist, and with the expected info
    BankStatement(1, balance=0).save()
    BankStatement(2, balance=100).save()

    # assert values are what we think they should be
    statement1 = BankStatement.get(1)
    statement2 = BankStatement.get(2)
    assert statement1.balance == 0
    assert statement2.balance == 100

    with TransactWrite(connection=connection) as transaction:
        # let the users send money to one another
        # create a credit line item to user 1's account
        transaction.save(
            LineItem(user_id=1, amount=50, currency='USD'),
            condition=(LineItem.user_id.does_not_exist()),
        )
        # create a debit to user 2's account
        transaction.save(
            LineItem(user_id=2, amount=-50, currency='USD'),
            condition=(LineItem.user_id.does_not_exist()),
        )

        # add credit to user 1's account
        transaction.update(statement1, actions=[BankStatement.balance.add(50)])
        # debit from user 2's account if they have enough in the bank
        transaction.update(
            statement2,
            actions=[BankStatement.balance.add(-50)],
            condition=(BankStatement.balance >= 50)
        )

    statement1.refresh()
    statement2.refresh()
    assert statement1.balance == statement2.balance == 50


@pytest.mark.ddblocal
def test_transact_write__one_of_each(connection):
    User(1).save()
    User(2).save()
    statement = BankStatement(1, balance=100, active=True)
    statement.save()

    with TransactWrite(connection=connection) as transaction:
        transaction.condition_check(User, 1, condition=(User.user_id.exists()))
        transaction.delete(User(2))
        transaction.save(LineItem(4, amount=100, currency='USD'), condition=(LineItem.user_id.does_not_exist()))
        transaction.update(
            statement,
            actions=[
                BankStatement.active.set(False),
                BankStatement.balance.set(0),
            ]
        )

    # confirming transaction correct and successful
    assert User.get(1)
    with pytest.raises(DoesNotExist):
        User.get(2)

    new_line_item = next(LineItem.query(4, scan_index_forward=False, limit=1), None)
    assert new_line_item
    assert new_line_item.amount == 100
    assert new_line_item.currency == 'USD'

    statement.refresh()
    assert not statement.active
    assert statement.balance == 0


@pytest.mark.ddblocal
def test_transaction_write_with_version_attribute(connection):
    foo1 = Foo(1)
    foo1.save()
    foo2 = Foo(2, star='bar')
    foo2.save()
    foo3 = Foo(3)
    foo3.save()

    with TransactWrite(connection=connection) as transaction:
        transaction.condition_check(Foo, 1, condition=(Foo.bar.exists()))
        transaction.delete(foo2)
        transaction.save(Foo(4))
        transaction.update(
            foo3,
            actions=[
                Foo.star.set('birdistheword'),
            ]
        )

    assert Foo.get(1).version == 1
    with pytest.raises(DoesNotExist):
        Foo.get(2)
    # Local object's version attribute is updated automatically.
    assert foo3.version == 2
    assert Foo.get(4).version == 1


@pytest.mark.ddblocal
def test_transaction_get_with_version_attribute(connection):
    Foo(11).save()
    Foo(12, star='bar').save()

    with TransactGet(connection=connection) as transaction:
        foo1_future = transaction.get(Foo, 11)
        foo2_future = transaction.get(Foo, 12)

    foo1 = foo1_future.get()
    assert foo1.version == 1
    foo2 = foo2_future.get()
    assert foo2.version == 1
    assert foo2.star == 'bar'


@pytest.mark.ddblocal
def test_transaction_write_with_version_attribute_condition_failure(connection):
    foo = Foo(21)
    foo.save()

    foo2 = Foo(21)

    with pytest.raises(TransactWriteError) as exc_info:
        with TransactWrite(connection=connection) as transaction:
            transaction.save(Foo(21))
    assert get_error_code(exc_info.value) == TRANSACTION_CANCELLED
    assert 'ConditionalCheckFailed' in get_error_message(exc_info.value)

    with pytest.raises(TransactWriteError) as exc_info:
        with TransactWrite(connection=connection) as transaction:
            transaction.update(
                foo2,
                actions=[
                    Foo.star.set('birdistheword'),
                ]
            )
    assert get_error_code(exc_info.value) == TRANSACTION_CANCELLED
    assert 'ConditionalCheckFailed' in get_error_message(exc_info.value)
    # Version attribute is not updated on failure.
    assert foo2.version is None

    with pytest.raises(TransactWriteError) as exc_info:
        with TransactWrite(connection=connection) as transaction:
            transaction.delete(foo2)
    assert get_error_code(exc_info.value) == TRANSACTION_CANCELLED
    assert 'ConditionalCheckFailed' in get_error_message(exc_info.value)
