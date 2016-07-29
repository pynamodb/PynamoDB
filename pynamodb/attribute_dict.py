import collections


class AttributeDict(collections.MutableMapping):
    """
    A dictionary that stores attributes by two keys
    """
    def __init__(self, *args, **kwargs):
        self._values = {}
        self._alt_values = {}
        self.update(dict(*args, **kwargs))

    def __getitem__(self, key):
        if key in self._alt_values:
            return self._alt_values[key]
        return self._values[key]

    def __setitem__(self, key, value):
        if value.attr_name is not None:
            self._values[value.attr_name] = value
        self._alt_values[key] = value

    def __delitem__(self, key):
        del self._values[key]

    def __iter__(self):
        return iter(self._alt_values)

    def __len__(self):
        return len(self._values)

    def aliased_attrs(self):
        return self._alt_values.items()
