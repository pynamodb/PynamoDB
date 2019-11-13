import six

if six.PY2:
    from inspect import getargspec as getfullargspec
    from imp import load_source
else:
    from inspect import getfullargspec
    from importlib.machinery import SourceFileLoader

load_source_compat = lambda name, path: load_source(name, path) if six.PY2 else SourceFileLoader(name, path).load_module()

__all__ = ('getfullargspec', 'load_source')

