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
    def key_names(self):
        # If the current page has a last_evaluated_key, use it to determine key attributes
        if self._last_evaluated_key:
            return self._last_evaluated_key.keys()

        # Use the table meta data to determine the key attributes
        table_meta = self._operation.im_self.get_meta_table()
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
        if self._first_iteration:
            # Not started iterating yet: there cannot be a last_evaluated_key
            return None

        if self._index == self._count:
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
