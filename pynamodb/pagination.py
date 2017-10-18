from pynamodb.constants import CAMEL_COUNT, ITEMS, LAST_EVALUATED_KEY


class ResultIterator(object):
    """
    ResultIterator handles Query and Scan result pagination.

    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
    """
    def __init__(self, operation, args, kwargs, map_fn=None, limit=None):
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._map_fn = map_fn
        self._limit = limit
        self._needs_execute = True
        self._total_count = 0

    def _execute(self):
        data = self._operation(*self._args, **self._kwargs)
        self._count = data[CAMEL_COUNT]
        self._items = data.get(ITEMS)  # not returned if 'Select' is set to 'COUNT'
        self._last_evaluated_key = data.get(LAST_EVALUATED_KEY)
        self._index = 0 if self._items else self._count
        self._total_count += self._count

    def __iter__(self):
        return self

    def __next__(self):
        if self._limit == 0:
            raise StopIteration

        if self._needs_execute:
            self._needs_execute = False
            self._execute()

        while self._index == self._count and self._last_evaluated_key:
            self._kwargs['exclusive_start_key'] = self._last_evaluated_key
            self._execute()

        if self._index == self._count:
            raise StopIteration

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
        return self._last_evaluated_key

    @property
    def total_count(self):
        return self._total_count
