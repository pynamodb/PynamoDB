"""
PynamoDB attributes
"""
import six
from six import add_metaclass
import json
from base64 import b64encode, b64decode
from delorean import Delorean, parse
from pynamodb.constants import (
    STRING, STRING_SHORT, NUMBER, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET,
    MAP, MAP_SHORT, LIST, LIST_SHORT, DEFAULT_ENCODING, BOOLEAN, ATTR_TYPE_MAP, NUMBER_SHORT, NULL
)
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


class AttributeContainer(object):

    _attributes = None
    _dynamo_to_python_attrs = None

    @classmethod
    def _initialize_attributes(cls):
        """
        Initialize attributes on the class.
        """
        cls._attributes = {}
        cls._dynamo_to_python_attrs = {}
        for item_name, instance in six.iteritems(cls.__dict__):
            if issubclass(type(instance), (Attribute,)):
                cls._attributes[item_name] = instance
                if instance.attr_name is not None:
                    cls._dynamo_to_python_attrs[instance.attr_name] = item_name
                else:
                    instance.attr_name = item_name

    @classmethod
    def _get_attributes(cls):
        """
        Returns the attributes of this class as a mapping from `python_attr_name` => `attribute`.

        :rtype: dict[str, Attribute]
        """
        if cls._attributes is None:
            cls._initialize_attributes()
        return cls._attributes

    @classmethod
    def _dynamo_to_python_attr(cls, dynamo_key):
        """
        Convert a DynamoDB attribute name to the internal Python name.

        This covers cases where an attribute name has been overridden via "attr_name".
        """
        if cls._attributes is None:
            cls._initialize_attributes()
        return cls._dynamo_to_python_attrs.get(dynamo_key, dynamo_key)

    def _set_defaults(self):
        """
        Sets and fields that provide a default value
        """
        for name, attr in self._get_attributes().items():
            default = attr.default
            if callable(default):
                value = default()
            else:
                value = default
            if value is not None:
                setattr(self, name, value)


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


class NullAttribute(Attribute):
    attr_type = NULL

    def serialize(self, value):
        return True

    def deserialize(self, value):
        return None


class MapAttributeMeta(type):
    def __init__(cls, name, bases, attrs):
        setattr(cls, '_attributes', None)


@add_metaclass(MapAttributeMeta)
class MapAttribute(AttributeContainer, Attribute):
    attr_type = MAP

    def __init__(self, hash_key=False, range_key=False, null=None, default=None, attr_name=None, **attrs):
        super(MapAttribute, self).__init__(hash_key=hash_key,
                                           range_key=range_key,
                                           null=null,
                                           default=default,
                                           attr_name=attr_name)
        self._get_attributes()  # Ensure attributes are always inited
        self.attribute_values = {}
        self._set_defaults()
        self._set_attributes(**attrs)

    def __iter__(self):
        return iter(self.attribute_values)

    def __getitem__(self, item):
        return self.attribute_values[item]

    def __getattr__(self, attr):
        return self.attribute_values[attr]

    def __set__(self, instance, value):
        if isinstance(value, collections.Mapping):
            value = type(self)(**value)
        return super(MapAttribute, self).__set__(instance, value)

    def _set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        for attr_name, value in six.iteritems(attrs):
            attribute = self._get_attributes().get(attr_name)
            if self.is_raw():
                self.attribute_values[attr_name] = value
            elif not attribute:
                raise AttributeError("Attribute {0} specified does not exist".format(attr_name))
            else:
                setattr(self, attr_name, value)

    def is_correctly_typed(self, key, attr):
        can_be_null = attr.null
        value = getattr(self, key)
        if can_be_null and value is None:
            return True
        if getattr(self, key) is None:
            raise ValueError("Attribute '{0}' cannot be None".format(key))
        return True  # TODO: check that the actual type of `value` meets requirements of `attr`

    def validate(self):
        return all(self.is_correctly_typed(k, v) for k, v in six.iteritems(self._get_attributes()))

    def serialize(self, values):
        rval = {}
        for k in values:
            v = values[k]
            attr_class = _get_class_for_serialize(v)
            attr_key = _get_key_for_serialize(v)
            if attr_class is None:
                continue

            # If this is a subclassed MapAttribute, there may be an alternate attr name
            attr = self._get_attributes().get(k)
            attr_name = attr.attr_name if attr else k

            rval[attr_name] = {attr_key: attr_class.serialize(v)}

        return rval

    def deserialize(self, values):
        """
        Decode as a dict.
        """
        deserialized_dict = dict()
        for k in values:
            v = values[k]
            attr_value = _get_value_for_deserialize(v)
            key = self._dynamo_to_python_attr(k)
            attr_class = self._get_deserialize_class(key, v)

            deserialized_dict[key] = attr_class.deserialize(attr_value)

        # If this is a subclass of a MapAttribute (i.e typed), instantiate an instance
        if not self.is_raw():
            return type(self)(**deserialized_dict)
        return deserialized_dict

    @classmethod
    def is_raw(cls):
        return cls == MapAttribute

    def as_dict(self):
        result = {}
        for key, value in six.iteritems(self.attribute_values):
            result[key] = value.as_dict() if isinstance(value, MapAttribute) else value
        return result

    @classmethod
    def _get_deserialize_class(cls, key, value):
        if not cls.is_raw():
            return cls._get_attributes().get(key)
        return _get_class_for_deserialize(value)

def _get_value_for_deserialize(value):
    return value[list(value.keys())[0]]


def _get_class_for_deserialize(value):
    value_type = list(value.keys())[0]
    if value_type not in DESERIALIZE_CLASS_MAP:
        raise ValueError('Unknown value: ' + str(value))
    return DESERIALIZE_CLASS_MAP[value_type]


def _get_class_for_serialize(value):
    if value is None:
        return NullAttribute()
    if isinstance(value, MapAttribute):
        return type(value)()
    value_type = type(value)
    if value_type not in SERIALIZE_CLASS_MAP:
        raise ValueError('Unknown value: {}'.format(value_type))
    return SERIALIZE_CLASS_MAP[value_type]


def _get_key_for_serialize(value):
    if value is None:
        return NullAttribute.attr_type
    if isinstance(value, MapAttribute):
        return MAP_SHORT
    value_type = type(value)
    if value_type not in SERIALIZE_KEY_MAP:
        raise ValueError('Unknown value: {}'.format(value_type))
    return SERIALIZE_KEY_MAP[value_type]


class ListAttribute(Attribute):
    attr_type = LIST
    element_type = None

    def __init__(self, hash_key=False, range_key=False, null=None, default=None, attr_name=None, of=None):
        super(ListAttribute, self).__init__(hash_key=hash_key,
                                            range_key=range_key,
                                            null=null,
                                            default=default,
                                            attr_name=attr_name)
        if of:
            if not issubclass(of, MapAttribute):
                raise ValueError("'of' must be subclass of MapAttribute")
            self.element_type = of

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
        Decode from list of AttributeValue types.
        """
        deserialized_lst = []
        for v in values:
            class_for_deserialize = self.element_type() if self.element_type else _get_class_for_deserialize(v)
            attr_value = _get_value_for_deserialize(v)
            deserialized_lst.append(class_for_deserialize.deserialize(attr_value))
        return deserialized_lst

DESERIALIZE_CLASS_MAP = {
    LIST_SHORT: ListAttribute(),
    NUMBER_SHORT: NumberAttribute(),
    STRING_SHORT: UnicodeAttribute(),
    BOOLEAN: BooleanAttribute(),
    MAP_SHORT: MapAttribute(),
    NULL: NullAttribute()
}

SERIALIZE_CLASS_MAP = {
    dict: MapAttribute(),
    list: ListAttribute(),
    set: ListAttribute(),
    bool: BooleanAttribute(),
    float: NumberAttribute(),
    int: NumberAttribute(),
    str: UnicodeAttribute(),
}


SERIALIZE_KEY_MAP = {
    dict: MAP_SHORT,
    list: LIST_SHORT,
    set: LIST_SHORT,
    bool: BOOLEAN,
    float: NUMBER_SHORT,
    int: NUMBER_SHORT,
    str: STRING_SHORT,
}


if six.PY2:
    SERIALIZE_CLASS_MAP[unicode] = UnicodeAttribute()
    SERIALIZE_CLASS_MAP[long] = NumberAttribute()
    SERIALIZE_KEY_MAP[unicode] = STRING_SHORT
    SERIALIZE_KEY_MAP[long] = NUMBER_SHORT
