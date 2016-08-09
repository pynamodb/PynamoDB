"""
PynamoDB attributes
"""
import six
import json
from base64 import b64encode, b64decode
from delorean import Delorean, parse
from pynamodb.constants import (
    STRING, NUMBER, BINARY, UTC, DATETIME_FORMAT, BINARY_SET, STRING_SET, NUMBER_SET,
    MAP, LIST, DEFAULT_ENCODING, ATTR_TYPE_MAP
)
from pynamodb.attribute_dict import AttributeDict


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

    def haha(self, hash_key=False,
                 range_key=False,
                 null=None,
                 default=None,
                 attr_name=None):
        self.default = default
        if null is not None:
            self.null = null
        self.is_hash_key = hash_key
        self.is_range_key = range_key
        if attr_name is not None:
            self.attr_name = attr_name

    def __set__(self, instance, value):
        #if isinstance(value, Attribute):
        #    return self
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


class BooleanAttribute(Attribute):
    """
    A class for boolean attributes

    This attribute type uses a number attribute to save space
    """
    attr_type = NUMBER

    def serialize(self, value):
        """
        Encodes True as 1, False as 0
        """
        if value is None:
            return None
        elif value:
            return json.dumps(1)
        else:
            return json.dumps(0)

    def deserialize(self, value):
        """
        Encode
        """
        return bool(json.loads(value))


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


from six import with_metaclass


class MapAttributeMeta(type):
    def __init__(cls, name, bases, attrs):
        setattr(cls, '_attributes', None)


#class MapAttribute(with_metaclass(MapAttributeMeta), Attribute):
class MapAttribute(with_metaclass(MapAttributeMeta, Attribute)):
#class MapAttribute(Attribute, with_metaclass(MapAttributeMeta)):
#class MapAttribute(Attribute):
    _attributes = None

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
            for item in dir(cls):
                try:
                    item_cls = getattr(getattr(cls, item), "__class__", None)
                except AttributeError:
                    continue
                if item_cls is None:
                    continue
                if issubclass(item_cls, (Attribute,)):
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
                setattr(self, attr_name, attrs.get(attr.attr_name))
            elif attr_name in attrs:
                setattr(self, attr_name, attrs.get(attr_name))

    def get_values(self):
        attributes = self._get_attributes()
        result = {}
        for k, v in attributes.iteritems():
            result[k] = getattr(self, k)
        return result

    def is_type_safe(self, key, value):
        return getattr(self, key) and type(getattr(self, key)) is not type(
            value)

    def validate(self):
        for key, value in self._get_attributes().iteritems():
            if not self.is_type_safe(key, value):
                return False
        return True

    attr_type = MAP

    def serialize(self, values):
        """
        Encode the given list of numbers into a list of AttributeValue types.
        """
        rval = dict()
        for k in values:
            v = values[k]
            if v is None:
                rval[k] = {'NULL': bool(1)}
            elif type(v) is dict:
                rval[k] = {'M': MapAttribute().serialize(v)}
            elif type(v) is list:
                rval[k] = {'L': ListAttribute().serialize(v)}
            elif type(v) is float or type(v) is int or type(v) is long:
                rval[k] = {'N': NumberAttribute().serialize(v)}
            elif type(v) is str or type(v) is unicode or type(v) is basestring:
                rval[k] = {'S': UnicodeAttribute().serialize(v)}
            elif type(v) is bool:
                value_to_dump = bool(0)
                if v:
                    value_to_dump = bool(1)

                rval[k] = {'BOOL': value_to_dump}
            else:
                raise Exception('Unknown value: ' + str(v))

        return rval

    def deserialize(self, values):
        """
        Decode numbers from list of AttributeValue types.
        """
        rval = dict()
        for k in values:
            v = values[k]
            if 'L' in v:
                rval[k] = ListAttribute().deserialize(v['L'])
            elif 'N' in v:
                rval[k] = NumberAttribute().deserialize(v['N'])
            elif 'S' in v:
                rval[k] = str(v['S'])
            elif 'NULL' in v:
                rval[k] = None
            elif 'BOOL' in v:
                rval[k] = bool(v['BOOL'])
            elif 'M' in v:
                rval[k] = MapAttribute().deserialize(v['M'])
            else:
                raise Exception('Unknown value: ' + str(v))
        return rval


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
            if v is None:
                rval += [{'NULL': bool(1)}]
            elif type(v) is dict:
                rval += [{'M': MapAttribute().serialize(v)}]
            elif type(v) is list or type(v) is set:  # no set embeded in List
                rval += [{'L': ListAttribute().serialize(v)}]
            elif type(v) is float or type(v) is int or type(v) is long:
                rval += [{'N': NumberAttribute().serialize(v)}]
            elif type(v) is bool:
                value_to_dump = bool(0)
                if v:
                    value_to_dump = bool(1)
                rval += [{'BOOL': value_to_dump}]
            elif type(v) is unicode or type(v) is str or type(v) is basestring:
                rval += [{'S': UnicodeAttribute().serialize(v)}]
            else:
                raise Exception('Unknown value: ' + str(v))
        return rval

    def deserialize(self, values):
        """
        Decode numbers from list of AttributeValue types.
        """
        rval = []
        # This should be a generic function that takes any AttributeValue and
        # translates it back to the Python type.
        for v in values:
            if 'L' in v:
                rval += [ListAttribute().deserialize(v['L'])]
            elif 'N' in v:
                rval += [NumberAttribute().deserialize(v['N'])]
            elif 'S' in v:
                rval += [UnicodeAttribute().deserialize(v['S'])]
            elif 'NULL' in v:
                rval += [None]
            elif 'BOOL' in v:
                rval += [BooleanAttribute().deserialize(v['BOOL'])]
            elif 'M' in v:
                rval += [MapAttribute().deserialize(v['M'])]
            else:
                raise Exception('Unknown value: ' + str(v))
        return rval
