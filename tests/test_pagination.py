import pytest
from typing import Any, Dict, List
from unittest.mock import Mock, call

from pynamodb.pagination import PageIterator, RateLimiter, ResultIterator


class MockTime():
    def __init__(self):
        self.current_time = 0.0

    def sleep(self, amount):
        self.current_time += amount

    def time(self):
        return self.current_time

    def increment_time(self, amount):
        self.current_time += amount


@pytest.mark.parametrize('end_marker', [{}, {'LastEvaluatedKey': {}}])
@pytest.mark.parametrize('items', [[], [{'id': {'S': 'item'}}]])
def test_page_iterator_stops_at_end(end_marker: Dict[str, Any], items: List[Dict[str, Any]]) -> None:
    page = {'Count': len(items), 'ScannedCount': len(items), 'Items': items, **end_marker}
    operation = Mock(return_value=page)
    pages = PageIterator(operation, (), {})

    assert next(pages) == page
    with pytest.raises(StopIteration):
        next(pages)
    operation.assert_called_once_with(exclusive_start_key=None)


def test_result_iterator_does_not_repeat_last_page() -> None:
    item = {'id': {'S': 'item'}}
    operation = Mock(return_value={
        'Count': 1, 'ScannedCount': 1, 'Items': [item], 'LastEvaluatedKey': {},
    })
    results = ResultIterator(operation, (), {})

    assert next(results) == item
    with pytest.raises(StopIteration):
        next(results)
    assert operation.call_count == 1


def test_result_iterator_continues_after_filtered_page() -> None:
    key = {'id': {'S': 'filtered'}}
    item = {'id': {'S': 'matching'}}
    operation = Mock(side_effect=[
        {'Count': 0, 'ScannedCount': 1, 'Items': [], 'LastEvaluatedKey': key},
        {'Count': 1, 'ScannedCount': 1, 'Items': [item]},
    ])
    results = ResultIterator(operation, (), {})

    assert list(results) == [item]
    assert operation.call_args_list == [
        call(exclusive_start_key=None), call(exclusive_start_key=key),
    ]


def test_rate_limiter_exceptions():
    with pytest.raises(ValueError):
        r = RateLimiter(0)

    with pytest.raises(ValueError):
        r = RateLimiter(-1)

    with pytest.raises(ValueError):
        r = RateLimiter(10)
        r.rate_limit = 0

    with pytest.raises(ValueError):
        r = RateLimiter(10)
        r.rate_limit = -1


def test_basic_rate_limiting():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)

    # 100 operations
    for i in range(0, 100):
        r.acquire()
        # Simulates an operation that takes 1 second
        mock_time.increment_time(1)
        r.consume(1)

    # Since the first acquire doesn't take time, thus we should be expecting (100-1) * 10 seconds = 990 delay
    # plus 1 for the last increment_time(1) operation
    assert mock_time.time() == 991.0


def test_basic_rate_limiting_small_increment():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)

    # 100 operations
    for i in range(0, 100):
        r.acquire()
        # Simulates an operation that takes 2 second
        mock_time.increment_time(2)
        r.consume(1)

    # Since the first acquire doesn't take time, thus we should be expecting (100-1) * 10 seconds = 990 delay
    # plus 2 for the last increment_time(2) operation
    assert mock_time.time() == 992.0


def test_basic_rate_limiting_large_increment():
    mock_time = MockTime()
    r = RateLimiter(0.1, mock_time)

    # 100 operations
    for i in range(0, 100):
        r.acquire()
        # Simulates an operation that takes 2 second
        mock_time.increment_time(11)
        r.consume(1)

    # The operation takes longer than the minimum wait, so rate limiting should have no effect
    assert mock_time.time() == 1100.0
