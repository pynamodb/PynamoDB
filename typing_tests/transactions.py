from typing_extensions import assert_type


def test_transactions() -> None:
    from pynamodb.transactions import TransactWrite
    with TransactWrite() as tx:
        assert_type(tx, TransactWrite)
