import time
import six
from pynamodb.constants import (CAMEL_COUNT, ITEMS, LAST_EVALUATED_KEY, SCANNED_COUNT,
                                CONSUMED_CAPACITY, TOTAL, CAPACITY_UNITS)


class RateLimiter(object):
    """
    RateLimiter limits operations to a pre-set rate of units/seconds

    Example:
        Initialize a RateLimiter with the desired rate
            rate_limiter = RateLimiter(rate_limit)

        Now, every time before calling an operation, call acquire()
            rate_limiter.acquire()

        And after an operation, update the number of units consumed
            rate_limiter.consume(units)

    """
    def __init__(self, rate_limit, time_module = None):
        """
        Initializes a RateLimiter object

        :param rate_limit: The desired rate
        :param time_module: Optional: the module responsible for calculating time. Intended to be used for testing purposes.
        """
        if rate_limit <= 0:
            raise ValueError("rate_limit must be greater than zero")
        self._rate_limit = rate_limit
        self._consumed = 0
        self._time_of_last_acquire = 0.0
        self._time_module = time_module or time

    def consume(self, units):
        """
        Records the amount of units consumed.

        :param units: Number of units consumed

        :return: None
        """
        self._consumed += units

    def acquire(self):
        """
        Sleeps the appropriate amount of time to follow the rate limit restriction

        :return: None
        """

        self._time_module.sleep(max(0, self._consumed/float(self.rate_limit) - (self._time_module.time()-self._time_of_last_acquire)))
        self._consumed = 0
        self._time_of_last_acquire = self._time_module.time()

    @property
    def rate_limit(self):
        """
        A limit of units per seconds
        """
        return self._rate_limit

    @rate_limit.setter
    def rate_limit(self, rate_limit):
        if rate_limit <= 0:
            raise ValueError("rate_limit must be greater than zero")
        self._rate_limit = rate_limit


class PageIterator(object):
    """
    PageIterator handles Query and Scan result pagination.

    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
    """
    def __init__(self, operation, args, kwargs, rate_limit = None):
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._first_iteration = True
        self._last_evaluated_key = kwargs.get('exclusive_start_key')
        self._total_scanned_count = 0
        self._rate_limiter = None
        if rate_limit:
            self._rate_limiter = RateLimiter(rate_limit)

    def __iter__(self):
        return self

    def __next__(self):
        if self._last_evaluated_key is None and not self._first_iteration:
            raise StopIteration()

        self._first_iteration = False

        self._kwargs['exclusive_start_key'] = self._last_evaluated_key

        if self._rate_limiter:
            self._rate_limiter.acquire()
            self._kwargs['return_consumed_capacity'] = TOTAL
        page = self._operation(*self._args, **self._kwargs)
        self._last_evaluated_key = page.get(LAST_EVALUATED_KEY)
        self._total_scanned_count += page[SCANNED_COUNT]

        if self._rate_limiter:
            consumed_capacity = page.get(CONSUMED_CAPACITY, {}).get(CAPACITY_UNITS, 0)
            self._rate_limiter.consume(consumed_capacity)

        return page

    def next(self):
        return self.__next__()

    @property
    def key_names(self):
        # If the current page has a last_evaluated_key, use it to determine key attributes
        if self._last_evaluated_key:
            return self._last_evaluated_key.keys()

        # Use the table meta data to determine the key attributes
        table_meta = six.get_method_self(self._operation).get_meta_table()  # type: ignore  # method_self cannot be None
        return table_meta.get_key_names(self._kwargs.get('index_name'))

    @property
    def page_size(self):
        return self._kwargs.get('limit')

    @page_size.setter
    def page_size(self, page_size):
        self._kwargs['limit'] = page_size

    @property
    def last_evaluated_key(self):
        return self._last_evaluated_key

    @property
    def total_scanned_count(self):
        return self._total_scanned_count


class ResultIterator(object):
    """
    ResultIterator handles Query and Scan item pagination.

    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
    """
    def __init__(self, operation, args, kwargs, map_fn=None, limit=None, rate_limit = None):
        self.page_iter = PageIterator(operation, args, kwargs, rate_limit)
        self._first_iteration = True
        self._map_fn = map_fn
        self._limit = limit
        self._total_count = 0

    def _get_next_page(self):
        page = next(self.page_iter)
        self._count = page[CAMEL_COUNT]
        self._items = page.get(ITEMS)  # not returned if 'Select' is set to 'COUNT'
        self._index = 0 if self._items else self._count
        self._total_count += self._count

    def __iter__(self):
        return self

    def __next__(self):
        if self._limit == 0:
            raise StopIteration

        if self._first_iteration:
            self._first_iteration = False
            self._get_next_page()

        while self._index == self._count:
            self._get_next_page()

        item = self._items[self._index]
        self._index += 1
        if self._limit is not None:
            self._limit -= 1
        if self._map_fn:
            item = self._map_fn(item)
        return item

    def next(self):
        return self.__next__()

    @property
    def last_evaluated_key(self):
        if self._first_iteration or self._index == self._count:
            # Not started iterating yet: return `exclusive_start_key` if set, otherwise expect None; or,
            # Entire page has been consumed: last_evaluated_key is whatever DynamoDB returned
            # It may correspond to the current item, or it may correspond to an item evaluated but not returned.
            return self.page_iter.last_evaluated_key

        # In the middle of a page of results: reconstruct a last_evaluated_key from the current item
        # The operation should be resumed starting at the last item returned, not the last item evaluated.
        # This can occur if the 'limit' is reached in the middle of a page.
        item = self._items[self._index - 1]
        return dict((key, item[key]) for key in self.page_iter.key_names)

    @property
    def total_count(self):
        return self._total_count
