from pynamodb.constants import CAMEL_COUNT, ITEMS, LAST_EVALUATED_KEY, SCANNED_COUNT


class PageIterator(object):
    """
    PageIterator handles Query and Scan result pagination.

    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html#Query.Pagination
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.Pagination
    """
    def __init__(self, operation, args, kwargs):
        self._operation = operation
        self._args = args
        self._kwargs = kwargs
        self._first_iteration = True
        self._last_evaluated_key = None
        self._total_scanned_count = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self._last_evaluated_key is None and not self._first_iteration:
            raise StopIteration()

        self._first_iteration = False

        self._kwargs['exclusive_start_key'] = self._last_evaluated_key
        page = self._operation(*self._args, **self._kwargs)
        self._last_evaluated_key = page.get(LAST_EVALUATED_KEY)
        self._total_scanned_count += page[SCANNED_COUNT]

        return page

    def next(self):
        return self.__next__()

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
    def __init__(self, operation, args, kwargs, map_fn=None, limit=None):
        self.page_iter = PageIterator(operation, args, kwargs)
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
        return self.page_iter.last_evaluated_key

    @property
    def total_count(self):
        return self._total_count
