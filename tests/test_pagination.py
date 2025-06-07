import pytest
from pynamodb.pagination import RateLimiter, PageIterator


class MockTime():
    def __init__(self):
        self.current_time = 0.0

    def sleep(self, amount):
        self.current_time += amount

    def time(self):
        return self.current_time

    def increment_time(self, amount):
        self.current_time += amount


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


def test_page_iterator_with_rate_limit():
    def mock_operation():
        pass

    args = None
    kwargs = {'exclusive_start_key': None}
    rate_limit = 0.1
    page_iter = PageIterator(mock_operation, args, kwargs, rate_limit)
    assert page_iter._rate_limiter.rate_limit == rate_limit


def test_page_iterator_page_size_getter():
    page_iter = PageIterator(None, None, {'limit': 10})
    assert page_iter.page_size == 10


def test_page_iterator_page_size_setter():
    page_iter = PageIterator(None, None, {})
    page_iter.page_size = 20
    assert page_iter._kwargs['limit'] == 20
