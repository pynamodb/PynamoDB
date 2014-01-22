"""
pynamodb attributes
"""
import six
import json
from delorean import Delorean, parse

from pynamodb.constants import (
    STRING, NUMBER, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET
)


class Attribute(object):
    """
    An attribute of a model
    """
    def __init__(self,
                 attr_type=str,
                 hash_key=False,
                 range_key=False,
                 null=False,
                 default=None
                 ):
        self.default = default
        self.null = null
        self.attr_type = attr_type
        self.is_hash_key = hash_key
        self.is_range_key = range_key

    def serialize(self, value):
        """
        This method should return a dynamodb compatible value
        """
        return value

    def deserialize(self, value):
        """
        Performs any needed deserialization on the value
        """
        return value


class SetMixin(object):
    """
    Adds (de)serialization methods
    """
    def serialize(self, value):
        """
        Serializes a set

        Because dynamodb doesn't store empty attributes,
        empty sets return None
        """
        if len(value):
            return [json.dumps(val) for val in sorted(value)]
        else:
            return None

    def deserialize(self, value):
        """
        Deserializes a set
        """
        return set([json.loads(val) for val in value])


class BinaryAttribute(Attribute):
    """
    A binary attribute
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', BINARY)
        super(BinaryAttribute, self).__init__(**kwargs)

    def serialize(self, value):
        """
        Returns a utf-8 encoded binary string
        """
        return six.b(value)


class BinarySetAttribute(SetMixin, Attribute):
    """
    A binary set
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', BINARY_SET)
        kwargs.setdefault('null', True)
        super(BinarySetAttribute, self).__init__(**kwargs)


class UnicodeSetAttribute(SetMixin, Attribute):
    """
    A unicode set
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', STRING_SET)
        kwargs.setdefault('null', True)
        super(UnicodeSetAttribute, self).__init__(**kwargs)


class UnicodeAttribute(Attribute):
    """
    A unicode attribute
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', STRING)
        super(UnicodeAttribute, self).__init__(**kwargs)

    def serialize(self, value):
        """
        Returns a unicode string
        """
        return six.u(value)


class NumberSetAttribute(SetMixin, Attribute):
    """
    A number set attribute
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', NUMBER_SET)
        kwargs.setdefault('null', True)
        super(NumberSetAttribute, self).__init__(**kwargs)


class NumberAttribute(Attribute):
    """
    A number attribute
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', NUMBER)
        super(NumberAttribute, self).__init__(**kwargs)


class UTCDateTimeAttribute(Attribute):
    """
    An attribute for storing a UTC Datetime
    """
    def __init__(self, **kwargs):
        kwargs.setdefault('attr_type', STRING)
        super(UTCDateTimeAttribute, self).__init__(**kwargs)

    def serialize(self, value):
        """
        Takes a datetime object and returns a string
        """
        fmt = Delorean(value, timezone=UTC).datetime.strftime(DATETIME_FORMAT)
        return six.u(fmt)

    def deserialize(self, value):
        """
        Takes a UTC datetime string and returns a datetime object
        """
        return parse(value).datetime
