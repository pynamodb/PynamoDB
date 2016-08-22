"""
PynamoDB attributes
"""
import six
from six import with_metaclass
import json
from base64 import b64encode, b64decode
from delorean import Delorean, parse
from pynamodb.constants import (
    STRING, STRING_SHORT, NUMBER, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET,
    MAP, MAP_SHORT, LIST, LIST_SHORT, DEFAULT_ENCODING, BOOLEAN, ATTR_TYPE_MAP, NUMBER_SHORT
)
from pynamodb.attribute_dict import AttributeDict
import collections


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


class MapAttributeMeta(type):
    def __init__(cls, name, bases, attrs):
        setattr(cls, '_attributes', None)


class MapAttribute(with_metaclass(MapAttributeMeta, Attribute)):
    attr_type = MAP

    def __init__(self, **attrs):

        hash_key = attrs.get('hash_key', False)
        range_key = attrs.get('range_key', False)
        null = attrs.get('null', None)
        default = attrs.get('default', None)
        attr_name = attrs.get('attr_name', None)

        super(MapAttribute, self).__init__(hash_key=hash_key,
                                           range_key=range_key,
                                           null=null,
                                           default=default,
                                           attr_name=attr_name)
        self.attribute_values = {}
        self._set_attributes(**attrs)

    def __iter__(self):
        return iter(self.attribute_values)

    def __getitem__(self, item):
        return self.attribute_values[item]

    @classmethod
    def _get_attributes(cls):
        """
        Returns the list of attributes for this class
        """
        if cls._attributes is None:
            cls._attributes = AttributeDict()
            for item in [attr for attr in dir(cls) if
                         isinstance(getattr(cls, attr), Attribute)]:
                instance = getattr(cls, item)
                if instance.attr_name is None:
                    instance.attr_name = item
                cls._attributes[item] = instance
        return cls._attributes

    def _set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        for attr_name, attr in self._get_attributes().aliased_attrs():
            if attr.attr_name in attrs:
                if type(attrs.get(attr.attr_name)) is type(attr) or not isinstance(attrs.get(attr_name), collections.Mapping):
                    setattr(self, attr_name, attrs.get(attr.attr_name))
                else:
                    sub_model = attrs.get(attr_name)
                    instance = type(attr)()
                    aliased_attrs = type(attr)._get_attributes().aliased_attrs()
                    for unaliased_key, v in sub_model.iteritems():
                        k = None
                        for python_accessible_key, aliased_v in aliased_attrs:
                            if aliased_v.attr_name == unaliased_key:
                                k = python_accessible_key
                        setattr(instance, k, v)
                    setattr(self, attr_name, instance)

            elif attr_name in attrs:
                setattr(self, attr_name, attrs.get(attr_name))

    def get_values(self):
        attributes = self._get_attributes()
        result = {}
        for k, v in attributes.iteritems():
            result[k] = getattr(self, k)
        return result

    def is_type_safe(self, key, value):
        can_be_null = value.null
        if can_be_null and getattr(self, key) is None:
            return True
        return getattr(self, key) and type(getattr(self, key)) is not type(
            value)

    def validate(self):
        for key, value in self._get_attributes().iteritems():
            if not self.is_type_safe(key, value):
                return False
        return True

    def serialize(self, values):
        """
        Encode the given list of numbers into a list of AttributeValue types.
        """
        rval = dict()
        for k in values:
            v = values[k]
            attr_class = _get_class_for_serialize(v)
            attr_key = _get_key_for_serialize(v)
            if attr_class is None:
                continue
            if attr_key is MAP_SHORT:
                rval[k] = [{attr_key: attr_class.serialize(v)}]
            else:
                rval[k] = {attr_key: attr_class.serialize(v)}

        return rval

    def deserialize(self, values):
        """
        Decode numbers from list of AttributeValue types.
        """
        rval = dict()
        for k in values:
            v = values[k]
            attr_class = _get_class_for_deserialize(v)
            attr_value = _get_value_for_deserialize(v)
            rval[k] = attr_class.deserialize(attr_value)
        return rval


def _get_value_for_deserialize(value):
    if LIST_SHORT in value:
        attr_value = value[LIST_SHORT]
    elif NUMBER_SHORT in value:
        attr_value = value[NUMBER_SHORT]
    elif STRING_SHORT in value:
        attr_value = value[STRING_SHORT]
    elif BOOLEAN in value:
        attr_value = value[BOOLEAN]
    elif MAP_SHORT in value:
        attr_value = value[MAP_SHORT]
    else:
        raise ValueError('Unknown value: ' + str(value))
    return attr_value


def _get_class_for_deserialize(value):
    if LIST_SHORT in value:
        return ListAttribute()
    elif NUMBER_SHORT in value:
        return NumberAttribute()
    elif STRING_SHORT in value:
        return UnicodeAttribute()
        #rval[k] = str(v['S'])
    elif BOOLEAN in value:
        return BooleanAttribute()
        #rval[k] = bool(v['BOOL'])
    elif MAP_SHORT in value:
        return MapAttribute()
    else:
        raise ValueError('Unknown value: ' + str(value))


def _get_class_for_serialize(value):
    if value is None:
        return None
    elif isinstance(value, dict):
        return MapAttribute()
    elif isinstance(value, list) or isinstance(value, set):
        return ListAttribute()
    elif isinstance(value, float) or isinstance(value, int):
        return NumberAttribute()
    elif isinstance(value, bool):
        return BooleanAttribute()
    elif isinstance(value, unicode) or isinstance(value, str) or isinstance(value, basestring):
        return UnicodeAttribute()
    elif issubclass(type(value), MapAttribute):
        return type(value)()
    else:
        raise Exception('Unknown value: ' + str(value))


def _get_key_for_serialize(value):
    if value is None:
        return None
    elif isinstance(value, dict):
        return MAP_SHORT
    elif isinstance(value, list) or isinstance(value, set):
        return LIST_SHORT
    elif isinstance(value, float) or isinstance(value, int):
        return NUMBER_SHORT
    elif isinstance(value, bool):
        return BOOLEAN
    elif isinstance(value, unicode) or isinstance(value, str) or isinstance(value, basestring):
        return STRING_SHORT
    elif issubclass(type(value), MapAttribute):
        return MAP_SHORT
    else:
        raise Exception('Unknown value: ' + str(value))


class ListAttribute(Attribute):
    attr_type = LIST
    element_type = None

    def __init__(self, **attrs):

        hash_key = attrs.get('hash_key', False)
        range_key = attrs.get('range_key', False)
        null = attrs.get('null', None)
        default = attrs.get('default', None)
        attr_name = attrs.get('attr_name', None)
        element_type = attrs.get('of', None)

        super(ListAttribute, self).__init__(hash_key=hash_key,
                                            range_key=range_key,
                                            null=null,
                                            default=default,
                                            attr_name=attr_name)
        if element_type:
            self.element_type = element_type

    def serialize(self, values):
        """
        Encode the given list of objects into a list of AttributeValue types.
        """
        rval = []
        for v in values:
            attr_class = _get_class_for_serialize(v)
            attr_key = _get_key_for_serialize(v)
            rval.append({attr_key: attr_class.serialize(v)})
        return rval

    def deserialize(self, values):
        """
        Decode numbers from list of AttributeValue types.
        """
        rval = []
        # This should be a generic function that takes any AttributeValue and
        # translates it back to the Python type.
        for v in values:
            attr_class = _get_class_for_deserialize(v)
            attr_value = _get_value_for_deserialize(v)
            rval.append(attr_class.deserialize(attr_value))
        return rval
