import six

if six.PY2:
    from inspect import getargspec as getfullargspec
    from imp import load_source
else:
    from inspect import getfullargspec
    from importlib.machinery import SourceFileLoader

def load_module(name, path):
    """Load module using the Python version compatible function."""
    if six.PY2:
        return load_source(name, path)
    return SourceFileLoader(name, path).load_module()

__all__ = ('getfullargspec', 'load_module')
