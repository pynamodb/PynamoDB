import inspect


def getmembers_issubclass(object, classinfo):
    def predicate(value):
        value_cls = getattr(value, '__class__', None)
        return value_cls and issubclass(value_cls, classinfo)
    return inspect.getmembers(object, predicate)
