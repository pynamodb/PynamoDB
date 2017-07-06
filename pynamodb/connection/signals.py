"""
Implements signals based on blinker if available, otherwise
falls silently back to a noop.

This implementation was taken from Flask:
https://github.com/pallets/flask/blob/master/flask/signals.py
"""
signals_available = False


class _FakeNamespace(object):
    def signal(self, name, doc=None):
        return _FakeSignal(name, doc)


class _FakeSignal(object):
    """
    If blinker is unavailable, create a fake class with the same
    interface that allows sending of signals but will fail with an
    error on anything else.  Instead of doing anything on send, it
    will just ignore the arguments and do nothing instead.
    """

    def __init__(self, name, doc=None):
        self.name = name
        self.__doc__ = doc

    def _fail(self, *args, **kwargs):
        raise RuntimeError('signalling support is unavailable '
                           'because the blinker library is '
                           'not installed.')

    send = lambda *a, **kw: None  # noqa
    connect = disconnect = has_receivers_for = receivers_for = \
        temporarily_connected_to = _fail
    del _fail


try:
    from blinker import Namespace
    signals_available = True
except ImportError: # pragma: no cover
    Namespace = _FakeNamespace

# The namespace for code signals.  If you are not PynamoDB code, do
# not put signals in here.  Create your own namespace instead.
_signals = Namespace()

pre_dynamodb_send = _signals.signal('pre_dynamodb_send')
post_dynamodb_send = _signals.signal('post_dynamodb_send')
