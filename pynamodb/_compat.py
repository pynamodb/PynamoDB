import six
import sys

if six.PY2:
    from inspect import getargspec as getfullargspec
else:
    from inspect import getfullargspec

def load_module(name, path):
    """Load module using the Python version compatible function."""
    if sys.version_info >= (3, 3):
        from importlib.machinery import SourceFileLoader
        return SourceFileLoader(name, path).load_module()
    else: 
        from imp import load_source
        return load_source(name, path)

__all__ = ('getfullargspec', 'load_module')
