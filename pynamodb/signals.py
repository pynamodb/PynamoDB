from pynamodb.connection import Connection
from pynamodb.connection.signals import post_boto_send, pre_boto_send

_pre_boto_registary = {}
_post_boto_registary = {}


def connect_pre_boto_calls(method):
    def wrapper(sender, **kwargs):
        return method(**kwargs)

    key = id(method)
    _pre_boto_registary[key] = wrapper
    pre_boto_send.connect(Connection, wrapper)


def connect_post_boto_calls(method):
    def wrapper(sender, **kwargs):
        import pdb; pdb.set_trace()
        return method(**kwargs)

    key = id(method)
    _post_boto_registary[key] = wrapper
    post_boto_send.connect(Connection, wrapper)


def disconnect_pre_boto_calls(method):
    key = id(method)
    wrapper = _pre_boto_registary.get(key)
    pre_boto_send.disconnect(wrapper)


def disconnect_post_boto_calls(method):
    key = id(method)
    wrapper = _post_boto_registary.get(key)
    pre_boto_send.disconnect(wrapper)
