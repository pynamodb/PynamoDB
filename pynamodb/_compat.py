import six

if six.PY2:
    from inspect import getargspec as getfullargspec
else:
    from inspect import getfullargspec

__all__ = ('getfullargspec',)
