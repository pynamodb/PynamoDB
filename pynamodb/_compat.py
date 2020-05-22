class FakeGenericMeta(type):
    """Poor man's Generic[T] that doesn't depend on typing. The real generics are in the type stubs."""
    def __getitem__(self, item):
        return self


__all__ = ('FakeGenericMeta',)
