import six

if six.PY2:
    from inspect import getargspec as getfullargspec
    from imp import load_source
else:
    from inspect import getfullargspec
    from importlib.machinery import SourceFileLoader

__all__ = ('getfullargspec',)
