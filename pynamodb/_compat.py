import six
import sys

if six.PY2:
    from inspect import getargspec as getfullargspec
else:
    from inspect import getfullargspec


class FakeGenericMeta(type):
    """Poor man's Generic[T] that doesn't depend on typing. The real generics are in the type stubs."""
    def __getitem__(self, item):
        return self


def load_module(name, path):
    """Load module using the Python version compatible function."""
    if sys.version_info >= (3, 3):
        from importlib.machinery import SourceFileLoader

        # Typeshed is incorrect in requiring a string arg to `load_module`,
        # as this works with no args or a None arg.
        # Even `load_module` is now deprecated, so we should update to just
        # using the following approach in >= python 3.5:
        # https://docs.python.org/3/library/importlib.html#importing-a-source-file-directly
        loader = SourceFileLoader(name, path)
        return loader.load_module()
    else:
        from imp import load_source
        return load_source(name, path)


__all__ = ('getfullargspec', 'FakeGenericMeta', 'load_module')
