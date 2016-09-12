"""
PynamoDB attributes
"""
import six
import json
from base64 import b64encode, b64decode
from delorean import Delorean, parse
from pynamodb.constants import (
    STRING, NUMBER, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET,
    DEFAULT_ENCODING, BOOLEAN, ATTR_TYPE_MAP, NUMBER_SHORT
)


class Attribute(object):
    """
    An attribute of a model
    """
    attr_name = None
    attr_type = None
    null = False

    def __init__(self,
                 hash_key=False,
                 range_key=False,
                 null=None,
                 default=None,
                 attr_name=None
                 ):
        self.default = default
        if null is not None:
            self.null = null
        self.is_hash_key = hash_key
        self.is_range_key = range_key
        if attr_name is not None:
            self.attr_name = attr_name

    def __set__(self, instance, value):
        if isinstance(value, Attribute):
            return self
        if instance:
            instance.attribute_values[self.attr_name] = value

    def __get__(self, instance, owner):
        if instance:
            return instance.attribute_values.get(self.attr_name, None)
        else:
            return self

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

    def get_value(self, value):
        serialized_dynamo_type = ATTR_TYPE_MAP[self.attr_type]
        return value.get(serialized_dynamo_type)


class SetMixin(object):
    """
    Adds (de)serialization methods for sets
    """
    def serialize(self, value):
        """
        Serializes a set

        Because dynamodb doesn't store empty attributes,
        empty sets return None
        """
        if value is not None:
            try:
                iter(value)
            except TypeError:
                value = [value]
            if len(value):
                return [json.dumps(val) for val in sorted(value)]
        return None

    def deserialize(self, value):
        """
        Deserializes a set
        """
        if value and len(value):
            return set([json.loads(val) for val in value])


class BinaryAttribute(Attribute):
    """
    A binary attribute
    """
    attr_type = BINARY

    def serialize(self, value):
        """
        Returns a base64 encoded binary string
        """
        return b64encode(value).decode(DEFAULT_ENCODING)

    def deserialize(self, value):
        """
        Returns a decoded string from base64
        """
        try:
            return b64decode(value.decode(DEFAULT_ENCODING))
        except AttributeError:
            return b64decode(value)


class BinarySetAttribute(SetMixin, Attribute):
    """
    A binary set
    """
    attr_type = BINARY_SET
    null = True

    def serialize(self, value):
        """
        Returns a base64 encoded binary string
        """
        if value and len(value):
            return [b64encode(val).decode(DEFAULT_ENCODING) for val in sorted(value)]
        else:
            return None

    def deserialize(self, value):
        """
        Returns a decoded string from base64
        """
        if value and len(value):
            return set([b64decode(val.encode(DEFAULT_ENCODING)) for val in value])


class UnicodeSetAttribute(SetMixin, Attribute):
    """
    A unicode set
    """
    attr_type = STRING_SET
    null = True

    def element_serialize(self, value):
        """
        This serializes unicode / strings out as unicode strings.
        It does not touch the value if it is already a unicode str
        :param value:
        :return:
        """
        if isinstance(value, six.text_type):
            return value
        return six.u(str(value))

    def element_deserialize(self, value):
        """
        This deserializes what we get from mongo back into a str
        Serialization previously json encoded strings. This caused them to have
        extra double quote (") characters. That no longer happens.
        This method allows both types of serialized values to be read
        :param value:
        :return:
        """
        result = value
        try:
            result = json.loads(value)
        except ValueError:
            # it's serialized in the new way so pass
            pass
        return result

    def serialize(self, value):
        if value is not None:
            try:
                iter(value)
            except TypeError:
                value = [value]
            if len(value):
                return [self.element_serialize(val) for val in sorted(value)]
        return None

    def deserialize(self, value):
        if value and len(value):
            return set([self.element_deserialize(val) for val in value])


class UnicodeAttribute(Attribute):
    """
    A unicode attribute
    """
    attr_type = STRING

    def serialize(self, value):
        """
        Returns a unicode string
        """
        if value is None or not len(value):
            return None
        elif isinstance(value, six.text_type):
            return value
        else:
            return six.u(value)


class JSONAttribute(Attribute):
    """
    A JSON Attribute

    Encodes JSON to unicode internally
    """
    attr_type = STRING

    def serialize(self, value):
        """
        Serializes JSON to unicode
        """
        if value is None:
            return None
        encoded = json.dumps(value)
        try:
            return unicode(encoded)
        except NameError:
            return encoded

    def deserialize(self, value):
        """
        Deserializes JSON
        """
        return json.loads(value, strict=False)


class LegacyBooleanAttribute(Attribute):
    """
    A class for legacy boolean attributes

    Previous versions of this library serialized bools as numbers.
    This class allows you to continue to use that functionality.
    """

    attr_type = NUMBER

    def serialize(self, value):
        if value is None:
            return None
        elif value:
            return json.dumps(1)
        else:
            return json.dumps(0)

    def deserialize(self, value):
        return bool(json.loads(value))

    def get_value(self, value):
        # we need this for the period in which you are upgrading
        # you can switch all BooleanAttributes to LegacyBooleanAttributes
        # this can read both but serializes as Numbers
        # once you've transitioned, you can then switch back to
        # BooleanAttribute and it will serialize the new fancy way
        value_to_deserialize = super(LegacyBooleanAttribute, self).get_value(value)
        if value_to_deserialize is None:
            value_to_deserialize = json.dumps(value.get(BOOLEAN, 0))
        return value_to_deserialize


class BooleanAttribute(Attribute):
    """
    A class for boolean attributes
    """
    attr_type = BOOLEAN

    def serialize(self, value):
        if value is None:
            return None
        elif value:
            return True
        else:
            return False

    def deserialize(self, value):
        return bool(value)

    def get_value(self, value):
        # we need this for legacy compatibility.
        # previously, BOOL was serialized as N
        value_to_deserialize = super(BooleanAttribute, self).get_value(value)
        if value_to_deserialize is None:
            value_to_deserialize = json.loads(value.get(NUMBER_SHORT, 0))
        return value_to_deserialize


class NumberSetAttribute(SetMixin, Attribute):
    """
    A number set attribute
    """
    attr_type = NUMBER_SET
    null = True


class NumberAttribute(Attribute):
    """
    A number attribute
    """
    attr_type = NUMBER

    def serialize(self, value):
        """
        Encode numbers as JSON
        """
        return json.dumps(value)

    def deserialize(self, value):
        """
        Decode numbers from JSON
        """
        return json.loads(value)


class UTCDateTimeAttribute(Attribute):
    """
    An attribute for storing a UTC Datetime
    """
    attr_type = STRING

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
        return parse(value, dayfirst=False).datetime
