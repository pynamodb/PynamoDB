class FakeGenericMeta(type):
    """
    Poor man's Generic[T] to work around the fact Python 3.6 typing.Generic doesn't like to coexist
    with custom metaclass.

    (Once our minimum requirement is Python 3.7 and up, we can replace this with typing.Generic.)
    """
    def __getitem__(self, item):
        return self


__all__ = ('FakeGenericMeta',)
