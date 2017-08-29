"""
PynamoDB attributes
"""
import six
from six import add_metaclass
import json
from base64 import b64encode, b64decode
from datetime import datetime
from dateutil.parser import parse
from dateutil.tz import tzutc
from inspect import getargspec
from pynamodb.constants import (
    STRING, STRING_SHORT, NUMBER, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET,
    MAP, MAP_SHORT, LIST, LIST_SHORT, DEFAULT_ENCODING, BOOLEAN, ATTR_TYPE_MAP, NUMBER_SHORT, NULL, SHORT_ATTR_TYPES
)
from pynamodb.expressions.condition import Path
import collections


class Attribute(object):
    """
    An attribute of a model
    """
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
        self.attr_name = attr_name

    def __set__(self, instance, value):
        if instance and not self._is_map_attribute_class_object(instance):
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            instance.attribute_values[attr_name] = value

    def __get__(self, instance, owner):
        if instance and not self._is_map_attribute_class_object(instance):
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            return instance.attribute_values.get(attr_name, None)
        else:
            return self

    def _is_map_attribute_class_object(self, instance):
        return isinstance(instance, MapAttribute) and not instance._is_attribute_container()

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

    # Condition Expression Support
    def __eq__(self, other):
        if other is None or isinstance(other, Attribute):  # handle object identity comparison
            return self is other
        return AttributePath(self).__eq__(other)

    def __ne__(self, other):
        if other is None or isinstance(other, Attribute):  # handle object identity comparison
            return self is not other
        return AttributePath(self).__ne__(other)

    def __lt__(self, other):
        return AttributePath(self).__lt__(other)

    def __le__(self, other):
        return AttributePath(self).__le__(other)

    def __gt__(self, other):
        return AttributePath(self).__gt__(other)

    def __ge__(self, other):
        return AttributePath(self).__ge__(other)

    def __getitem__(self, idx):
        return AttributePath(self)[idx]

    def between(self, lower, upper):
        return AttributePath(self).between(lower, upper)

    def is_in(self, *values):
        return AttributePath(self).is_in(*values)

    def exists(self):
        return AttributePath(self).exists()

    def does_not_exist(self):
        return AttributePath(self).does_not_exist()

    def is_type(self):
        # What makes sense here? Are we using this to check if deserialization will be successful?
        return AttributePath(self).is_type(ATTR_TYPE_MAP[self.attr_type])

    def startswith(self, prefix):
        return AttributePath(self).startswith(prefix)

    def contains(self, item):
        return AttributePath(self).contains(item)


class AttributePath(Path):

    def __init__(self, attribute):
        super(AttributePath, self).__init__(attribute.attr_name, attribute_name=True)
        self.attribute = attribute

    def __getitem__(self, idx):
        if self.attribute.attr_type != LIST:  # only list elements support the list dereference operator
            raise TypeError("'{0}' object has no attribute __getitem__".format(self.attribute.__class__.__name__))
        return super(AttributePath, self).__getitem__(idx)

    def contains(self, item):
        if self.attribute.attr_type in [BINARY_SET, NUMBER_SET, STRING_SET]:
            # Set attributes assume the values to be serialized are sets.
            (attr_type, attr_value), = self._serialize([item]).items()
            item = {attr_type[0]: attr_value[0]}
        return super(AttributePath, self).contains(item)

    def _serialize(self, value):
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in SHORT_ATTR_TYPES:
            return value
        if self.attribute.attr_type == LIST and not isinstance(value, list):
            # List attributes assume the values to be serialized are lists.
            return self.attribute.serialize([value])[0]
        if self.attribute.attr_type == MAP and not isinstance(value, dict):
            # Map attributes assume the values to be serialized are maps.
            return self.attribute.serialize({'': value})['']
        return {ATTR_TYPE_MAP[self.attribute.attr_type]: self.attribute.serialize(value)}


class AttributeContainerMeta(type):

    def __init__(cls, name, bases, attrs):
        super(AttributeContainerMeta, cls).__init__(name, bases, attrs)
        AttributeContainerMeta._initialize_attributes(cls)

    @staticmethod
    def _initialize_attributes(cls):
        """
        Initialize attributes on the class.
        """
        cls._attributes = {}
        cls._dynamo_to_python_attrs = {}

        for item_name in dir(cls):
            try:
                item_cls = getattr(getattr(cls, item_name), "__class__", None)
            except AttributeError:
                continue

            if item_cls is None:
                continue

            if issubclass(item_cls, Attribute):
                instance = getattr(cls, item_name)
                if isinstance(instance, MapAttribute):
                    # MapAttribute instances that are class attributes of an AttributeContainer class
                    # should behave like an Attribute instance and not an AttributeContainer instance.
                    instance._make_attribute()

                cls._attributes[item_name] = instance
                if instance.attr_name is not None:
                    cls._dynamo_to_python_attrs[instance.attr_name] = item_name
                else:
                    instance.attr_name = item_name


@add_metaclass(AttributeContainerMeta)
class AttributeContainer(object):

    def __init__(self, **attributes):
        # The `attribute_values` dictionary is used by the Attribute data descriptors in cls._attributes
        # to store the values that are bound to this instance. Attributes store values in the dictionary
        # using the `python_attr_name` as the dictionary key. "Raw" (i.e. non-subclassed) MapAttribute
        # instances do not have any Attributes defined and instead use this dictionary to store their
        # collection of name-value pairs.
        self.attribute_values = {}
        self._set_defaults()
        self._set_attributes(**attributes)

    @classmethod
    def _get_attributes(cls):
        """
        Returns the attributes of this class as a mapping from `python_attr_name` => `attribute`.

        :rtype: dict[str, Attribute]
        """
        return cls._attributes

    @classmethod
    def _dynamo_to_python_attr(cls, dynamo_key):
        """
        Convert a DynamoDB attribute name to the internal Python name.

        This covers cases where an attribute name has been overridden via "attr_name".
        """
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

    def _set_attributes(self, **attributes):
        """
        Sets the attributes for this object
        """
        for attr_name, attr_value in six.iteritems(attributes):
            if attr_name not in self._get_attributes():
                raise ValueError("Attribute {0} specified does not exist".format(attr_name))
            setattr(self, attr_name, attr_value)


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
        return value

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
        if value.tzinfo is None:
            value = value.replace(tzinfo=tzutc())
        fmt = value.astimezone(tzutc()).strftime(DATETIME_FORMAT)
        return six.u(fmt)

    def deserialize(self, value):
        """
        Takes a UTC datetime string and returns a datetime object
        """
        # First attempt to parse the datetime with the datetime format used
        # by default when storing UTCDateTimeAttributes.  This is signifantly
        # faster than always going through dateutil.
        try:
            return datetime.strptime(value, DATETIME_FORMAT)
        except ValueError:
            return parse(value)


class NullAttribute(Attribute):
    attr_type = NULL

    def serialize(self, value):
        return True

    def deserialize(self, value):
        return None


class MapAttributeMeta(AttributeContainerMeta):
    """
    This is only here for backwards compatibility: i.e. so type(MapAttribute) == MapAttributeMeta
    """


@add_metaclass(MapAttributeMeta)
class MapAttribute(Attribute, AttributeContainer):
    """
    A Map Attribute

    The MapAttribute class can be used to store a JSON document as "raw" name-value pairs, or
    it can be subclassed and the document fields represented as class attributes using Attribute instances.

    To support the ability to subclass MapAttribute and use it as an AttributeContainer, instances of
    MapAttribute behave differently based both on where they are instantiated and on their type.
    Because of this complicated behavior, a bit of an introduction is warranted.

    Models that contain a MapAttribute define its properties using a class attribute on the model.
    For example, below we define "MyModel" which contains a MapAttribute "my_map":

    class MyModel(Model):
       my_map = MapAttribute(attr_name="dynamo_name", default={})

    When instantiated in this manner (as a class attribute of an AttributeContainer class), the MapAttribute
    class acts as an instance of the Attribute class. The instance stores data about the attribute (in this
    example the dynamo name and default value), and acts as a data descriptor, storing any value bound to it
    on the `attribute_values` dictionary of the containing instance (in this case an instance of MyModel).

    Unlike other Attribute types, the value that gets bound to the containing instance is a new instance of
    MapAttribute, not an instance of the primitive type. For example, a UnicodeAttribute stores strings in
    the `attribute_values` of the containing instance; a MapAttribute does not store a dict but instead stores
    a new instance of itself. This difference in behavior is necessary when subclassing MapAttribute in order
    to access the Attribute data descriptors that represent the document fields.

    For example, below we redefine "MyModel" to use a subclass of MapAttribute as "my_map":

    class MyMapAttribute(MapAttribute):
        my_internal_map = MapAttribute()

    class MyModel(Model):
        my_map = MyMapAttribute(attr_name="dynamo_name", default = {})

    In order to set the value of my_internal_map on an instance of MyModel we need the bound value for "my_map"
    to be an instance of MapAttribute so that it acts as a data descriptor:

    MyModel().my_map.my_internal_map = {'foo': 'bar'}

    That is the attribute access of "my_map" must return a MyMapAttribute instance and not a dict.

    When an instance is used in this manner (bound to an instance of an AttributeContainer class),
    the MapAttribute class acts as an AttributeContainer class itself. The instance does not store data
    about the attribute, and does not act as a data descriptor. The instance stores name-value pairs in its
    internal `attribute_values` dictionary.

    Thus while MapAttribute multiply inherits from Attribute and AttributeContainer, a MapAttribute instance
    does not behave as both an Attribute AND an AttributeContainer. Rather an instance of MapAttribute behaves
    EITHER as an Attribute OR as an AttributeContainer, depending on where it was instantiated.

    So, how do we create this dichotomous behavior? Using the AttributeContainerMeta metaclass.
    All MapAttribute instances are initialized as AttributeContainers only. During construction of
    AttributeContainer classes (subclasses of MapAttribute and Model), any instances that are class attributes
    are transformed from AttributeContainers to Attributes (via the `_make_attribute` method call).
    """
    attr_type = MAP

    attribute_args = getargspec(Attribute.__init__).args[1:]

    def __init__(self, **attributes):
        # Store the kwargs used by Attribute.__init__ in case `_make_attribute` is called.
        self.attribute_kwargs = dict((arg, attributes.pop(arg)) for arg in self.attribute_args if arg in attributes)

        # Assume all instances should behave like an AttributeContainer. Instances that are intended to be
        # used as Attributes will be transformed by AttributeContainerMeta during creation of the containing class.
        # Because of this do not use MRO or cooperative multiple inheritance, call the parent class directly.
        AttributeContainer.__init__(self, **attributes)

        # It is possible that attributes names can collide with argument names of Attribute.__init__.
        # Assume that this is the case if any of the following are true:
        #   - the user passed in other attributes that did not match any argument names
        #   - this is "raw" (i.e. non-subclassed) MapAttribute instance and attempting to store the attributes
        #     cannot raise a ValueError (if this assumption is wrong, calling `_make_attribute` removes them)
        #   - the names of all attributes in self.attribute_kwargs match attributes defined on the class
        if self.attribute_kwargs and (
                attributes or self.is_raw() or all(arg in self._get_attributes() for arg in self.attribute_kwargs)):
            self._set_attributes(**self.attribute_kwargs)

    def _is_attribute_container(self):
        # Determine if this instance is being used as an AttributeContainer or an Attribute.
        # AttributeContainer instances have an internal `attribute_values` dictionary that is removed
        # by the `_make_attribute` call during initialization of the containing class.
        return 'attribute_values' in self.__dict__

    def _make_attribute(self):
        # WARNING! This function is only intended to be called from the AttributeContainerMeta metaclass.
        if not self._is_attribute_container():
            return
        # During initialization the kwargs were stored in `attribute_kwargs`. Remove them and re-initialize the class.
        kwargs = self.attribute_kwargs
        del self.attribute_kwargs
        del self.attribute_values
        Attribute.__init__(self, **kwargs)

    def __iter__(self):
        return iter(self.attribute_values)

    def __getitem__(self, item):
        return self.attribute_values[item]

    def __getattr__(self, attr):
        # This should only be called for "raw" (i.e. non-subclassed) MapAttribute instances.
        # MapAttribute subclasses should access attributes via the Attribute descriptors.
        if self.is_raw() and self._is_attribute_container():
            try:
                return self.attribute_values[attr]
            except KeyError:
                pass
        raise AttributeError("'{0}' has no attribute '{1}'".format(self.__class__.__name__, attr))

    def __setattr__(self, name, value):
        # "Raw" (i.e. non-subclassed) instances set their name-value pairs in the `attribute_values` dictionary.
        # MapAttribute subclasses should set attributes via the Attribute descriptors.
        if self.is_raw() and self._is_attribute_container():
            self.attribute_values[name] = value
        else:
            object.__setattr__(self, name, value)

    def __set__(self, instance, value):
        if isinstance(value, collections.Mapping):
            value = type(self)(**value)
        return super(MapAttribute, self).__set__(instance, value)

    def _set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        if self.is_raw():
            for name, value in six.iteritems(attrs):
                setattr(self, name, value)
        else:
            super(MapAttribute, self)._set_attributes(**attrs)

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
            attr_class = self._get_serialize_class(k, v)
            if attr_class is None:
                continue
            if attr_class.attr_type:
                attr_key = ATTR_TYPE_MAP[attr_class.attr_type]
            else:
                attr_key = _get_key_for_serialize(v)

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
            deserialized_value = None
            if attr_value is not None:
                deserialized_value = attr_class.deserialize(attr_value)

            deserialized_dict[key] = deserialized_value

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
    def _get_serialize_class(cls, key, value):
        if not cls.is_raw():
            return cls._get_attributes().get(key)
        return _get_class_for_serialize(value)

    @classmethod
    def _get_deserialize_class(cls, key, value):
        if not cls.is_raw():
            return cls._get_attributes().get(key)
        return _get_class_for_deserialize(value)


def _get_value_for_deserialize(value):
    key = next(iter(value.keys()))
    if key == NULL:
        return None
    return value[key]


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
            attr_class = (self.element_type()
                          if self.element_type
                          else _get_class_for_serialize(v))
            if attr_class.attr_type:
                attr_key = ATTR_TYPE_MAP[attr_class.attr_type]
            else:
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
