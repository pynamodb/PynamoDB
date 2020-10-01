"""
PynamoDB attributes
"""
import calendar
import collections.abc
import json
import time
import warnings
from base64 import b64encode, b64decode
from copy import deepcopy
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from inspect import getfullargspec
from inspect import getmembers
from typing import Any, Callable, Dict, Generic, List, Mapping, Optional, TypeVar, Type, Union, Set, overload, Iterable
from typing import TYPE_CHECKING

from pynamodb._compat import GenericMeta
from pynamodb.constants import BINARY
from pynamodb.constants import BINARY_SET
from pynamodb.constants import BOOLEAN
from pynamodb.constants import DATETIME_FORMAT
from pynamodb.constants import DEFAULT_ENCODING
from pynamodb.constants import LIST
from pynamodb.constants import MAP
from pynamodb.constants import NULL
from pynamodb.constants import NUMBER
from pynamodb.constants import NUMBER_SET
from pynamodb.constants import STRING
from pynamodb.constants import STRING_SET
from pynamodb.exceptions import AttributeDeserializationError
from pynamodb.exceptions import AttributeNullError
from pynamodb.expressions.operand import Path


if TYPE_CHECKING:
    from pynamodb.expressions.condition import (
        BeginsWith, Between, Comparison, Contains, NotExists, Exists, In
    )
    from pynamodb.expressions.operand import (
        _Decrement, _IfNotExists, _Increment, _ListAppend
    )
    from pynamodb.expressions.update import (
        AddAction, DeleteAction, RemoveAction, SetAction
    )


_T = TypeVar('_T')
_KT = TypeVar('_KT', bound=str)
_VT = TypeVar('_VT')
_MT = TypeVar('_MT', bound='MapAttribute')
_ACT = TypeVar('_ACT', bound = 'AttributeContainer')

_A = TypeVar('_A', bound='Attribute')


class Attribute(Generic[_T]):
    """
    An attribute of a model
    """
    attr_type: str
    null = False

    def __init__(
        self,
        hash_key: bool = False,
        range_key: bool = False,
        null: Optional[bool] = None,
        default: Optional[Union[_T, Callable[..., _T]]] = None,
        default_for_new: Optional[Union[Any, Callable[..., _T]]] = None,
        attr_name: Optional[str] = None,
    ) -> None:
        if default and default_for_new:
            raise ValueError("An attribute cannot have both default and default_for_new parameters")
        self.default = default
        # This default is only set for new objects (ie: it's not set for re-saved objects)
        self.default_for_new = default_for_new

        if null is not None:
            self.null = null
        self.is_hash_key = hash_key
        self.is_range_key = range_key

        # __set_name__ will ensure this is a string
        self.attr_path: List[str] = [attr_name]  # type: ignore

    @property
    def attr_name(self) -> str:
        return self.attr_path[-1]

    @attr_name.setter
    def attr_name(self, value: str) -> None:
        self.attr_path[-1] = value

    def __set__(self, instance: Any, value: Optional[_T]) -> None:
        if instance and not self._is_map_attribute_class_object(instance):
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            instance.attribute_values[attr_name] = value

    @overload
    def __get__(self: _A, instance: None, owner: Any) -> _A: ...

    @overload
    def __get__(self: _A, instance: Any, owner: Any) -> _T: ...

    def __get__(self: _A, instance: Any, owner: Any) -> Union[_A, _T]:
        if self._is_map_attribute_class_object(instance):
            # MapAttribute class objects store a local copy of the attribute with `attr_path` set to the document path.
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            return instance.__dict__.get(attr_name, None) or self
        elif instance:
            attr_name = instance._dynamo_to_python_attrs.get(self.attr_name, self.attr_name)
            return instance.attribute_values.get(attr_name, None)
        else:
            return self

    def __set_name__(self, owner: Type[Any], name: str) -> None:
        self.attr_name = self.attr_name or name

    def _is_map_attribute_class_object(self, instance: 'Attribute') -> bool:
        return isinstance(instance, MapAttribute) and not instance._is_attribute_container()

    def serialize(self, value: Any) -> Any:
        """
        This method should return a dynamodb compatible value
        """
        return value

    def deserialize(self, value: Any) -> Any:
        """
        Performs any needed deserialization on the value
        """
        return value

    def get_value(self, value: Dict[str, Any]) -> Any:
        if self.attr_type not in value:
            raise AttributeDeserializationError(self.attr_name, self.attr_type)
        return value[self.attr_type]

    def __iter__(self):
        # Because we define __getitem__ below for condition expression support
        raise TypeError("'{}' object is not iterable".format(self.__class__.__name__))

    # Condition Expression Support
    def __eq__(self, other: Any) -> 'Comparison':  # type: ignore
        if isinstance(other, MapAttribute) and other._is_attribute_container():
            return Path(self).__eq__(other)
        if other is None or isinstance(other, Attribute):  # handle object identity comparison
            return self is other  # type: ignore
        return Path(self).__eq__(other)

    def __ne__(self, other: Any) -> 'Comparison':  # type: ignore
        if isinstance(other, MapAttribute) and other._is_attribute_container():
            return Path(self).__ne__(other)
        if other is None or isinstance(other, Attribute):  # handle object identity comparison
            return self is not other  # type: ignore
        return Path(self).__ne__(other)

    def __lt__(self, other: Any) -> 'Comparison':
        return Path(self).__lt__(other)

    def __le__(self, other: Any) -> 'Comparison':
        return Path(self).__le__(other)

    def __gt__(self, other: Any) -> 'Comparison':
        return Path(self).__gt__(other)

    def __ge__(self, other: Any) -> 'Comparison':
        return Path(self).__ge__(other)

    def __getitem__(self, item: Union[int, str]) -> Path:
        return Path(self).__getitem__(item)

    def between(self, lower: Any, upper: Any) -> 'Between':
        return Path(self).between(lower, upper)

    def is_in(self, *values: _T) -> 'In':
        return Path(self).is_in(*values)

    def exists(self) -> 'Exists':
        return Path(self).exists()

    def does_not_exist(self) -> 'NotExists':
        return Path(self).does_not_exist()

    def is_type(self):
        # What makes sense here? Are we using this to check if deserialization will be successful?
        return Path(self).is_type(self.attr_type)

    def startswith(self, prefix: str) -> 'BeginsWith':
        return Path(self).startswith(prefix)

    def contains(self, item: Any) -> 'Contains':
        return Path(self).contains(item)

    # Update Expression Support
    def __add__(self, other: Any) -> '_Increment':
        return Path(self).__add__(other)

    def __radd__(self, other: Any) -> '_Increment':
        return Path(self).__radd__(other)

    def __sub__(self, other: Any) -> '_Decrement':
        return Path(self).__sub__(other)

    def __rsub__(self, other: Any) -> '_Decrement':
        return Path(self).__rsub__(other)

    def __or__(self, other: Any) -> '_IfNotExists':
        return Path(self).__or__(other)

    def append(self, other: Iterable) -> '_ListAppend':
        return Path(self).append(other)

    def prepend(self, other: Iterable) -> '_ListAppend':
        return Path(self).prepend(other)

    def set(self, value: Any) -> 'SetAction':
        return Path(self).set(value)

    def remove(self) -> 'RemoveAction':
        return Path(self).remove()

    def add(self, *values: Any) -> 'AddAction':
        return Path(self).add(*values)

    def delete(self, *values: Any) -> 'DeleteAction':
        return Path(self).delete(*values)


class AttributeContainerMeta(GenericMeta):

    def __new__(cls, name, bases, namespace, discriminator=None):
        # Defined so that the discriminator can be set in the class definition.
        return super().__new__(cls, name, bases, namespace)

    def __init__(self, name, bases, namespace, discriminator=None):
        super().__init__(name, bases, namespace)
        AttributeContainerMeta._initialize_attributes(self, discriminator)

    @staticmethod
    def _initialize_attributes(cls, discriminator_value):
        """
        Initialize attributes on the class.
        """
        cls._attributes = {}
        cls._dynamo_to_python_attrs = {}

        for name, attribute in getmembers(cls, lambda o: isinstance(o, Attribute)):
            cls._attributes[name] = attribute
            if attribute.attr_name != name:
                cls._dynamo_to_python_attrs[attribute.attr_name] = name

        # Register the class with the discriminator if necessary.
        discriminators = [name for name, attr in cls._attributes.items() if isinstance(attr, DiscriminatorAttribute)]
        if len(discriminators) > 1:
            raise ValueError("{} has more than one discriminator attribute: {}".format(
                cls.__name__, ", ".join(discriminators)))
        cls._discriminator = discriminators[0] if discriminators else None
        if discriminator_value is not None:
            if not cls._discriminator:
                raise ValueError("{} does not have a discriminator attribute".format(cls.__name__))
            cls._attributes[cls._discriminator].register_class(cls, discriminator_value)


class AttributeContainer(metaclass=AttributeContainerMeta):

    def __init__(self, _user_instantiated: bool = True, **attributes: Attribute) -> None:
        # The `attribute_values` dictionary is used by the Attribute data descriptors in cls._attributes
        # to store the values that are bound to this instance. Attributes store values in the dictionary
        # using the `python_attr_name` as the dictionary key. "Raw" (i.e. non-subclassed) MapAttribute
        # instances do not have any Attributes defined and instead use this dictionary to store their
        # collection of name-value pairs.
        self.attribute_values: Dict[str, Any] = {}
        self._set_discriminator()
        self._set_defaults(_user_instantiated=_user_instantiated)
        self._set_attributes(**attributes)

    @classmethod
    def _get_attributes(cls) -> Dict[str, Attribute]:
        """
        Returns the attributes of this class as a mapping from `python_attr_name` => `attribute`.
        """
        warnings.warn("`Model._get_attributes` is deprecated in favor of `Model.get_attributes` now")
        return cls.get_attributes()

    @classmethod
    def get_attributes(cls) -> Dict[str, Attribute]:
        """
        Returns the attributes of this class as a mapping from `python_attr_name` => `attribute`.

        :rtype: dict[str, Attribute]
        """
        return cls._attributes  # type: ignore

    @classmethod
    def _dynamo_to_python_attr(cls, dynamo_key: str) -> str:
        """
        Convert a DynamoDB attribute name to the internal Python name.

        This covers cases where an attribute name has been overridden via "attr_name".
        """
        return cls._dynamo_to_python_attrs.get(dynamo_key, dynamo_key)  # type: ignore

    @classmethod
    def _get_discriminator_attribute(cls) -> Optional['DiscriminatorAttribute']:
        return cls.get_attributes()[cls._discriminator] if cls._discriminator else None  # type: ignore

    def _set_discriminator(self) -> None:
        discriminator_attr = self._get_discriminator_attribute()
        if discriminator_attr and discriminator_attr.get_discriminator(self.__class__) is not None:
            setattr(self, self._discriminator, self.__class__)  # type: ignore

    def _set_defaults(self, _user_instantiated: bool = True) -> None:
        """
        Sets and fields that provide a default value
        """
        for name, attr in self.get_attributes().items():
            if _user_instantiated and attr.default_for_new is not None:
                default = attr.default_for_new
            else:
                default = attr.default
            if callable(default):
                value = default()
            else:
                value = default
            if value is not None:
                setattr(self, name, value)

    def _set_attributes(self, **attributes: Attribute) -> None:
        """
        Sets the attributes for this object
        """
        for attr_name, attr_value in attributes.items():
            if attr_name not in self.get_attributes():
                raise ValueError("Attribute {} specified does not exist".format(attr_name))
            setattr(self, attr_name, attr_value)

    def _container_serialize(self, null_check: bool = True) -> Dict[str, Dict[str, Any]]:
        """
        Serialize attribute values for DynamoDB
        """
        attribute_values: Dict[str, Dict[str, Any]] = {}
        for name, attr in self.get_attributes().items():
            value = getattr(self, name)
            try:
                if isinstance(value, MapAttribute) and not value.validate(null_check=null_check):
                    raise ValueError("Attribute '{}' is not correctly typed".format(name))
            except AttributeNullError as e:
                e.prepend_path(name)
                raise

            if value is not None:
                if isinstance(attr, MapAttribute):
                    attr_value = attr.serialize(value, null_check=null_check)
                else:
                    attr_value = attr.serialize(value)
            else:
                attr_value = None
            if null_check and attr_value is None and not attr.null:
                raise AttributeNullError(name)

            if attr_value is not None:
                attribute_values[attr.attr_name] = {attr.attr_type: attr_value}
        return attribute_values

    def _container_deserialize(self, attribute_values: Dict[str, Dict[str, Any]]) -> None:
        """
        Sets attributes sent back from DynamoDB on this object
        """
        self.attribute_values = {}
        self._set_discriminator()
        self._set_defaults(_user_instantiated=False)
        for name, attr in self.get_attributes().items():
            attribute_value = attribute_values.get(attr.attr_name)
            if attribute_value and NULL not in attribute_value:
                value = attr.deserialize(attr.get_value(attribute_value))
                setattr(self, name, value)

    @classmethod
    def _update_attribute_types(cls, attribute_values: Dict[str, Dict[str, Any]]):
        """
        Update the attribute types in the attribute values dictionary to disambiguate json string and array types
        """
        for attr in cls.get_attributes().values():
            attribute_value = attribute_values.get(attr.attr_name)
            if attribute_value:
                AttributeContainer._coerce_attribute_type(attr.attr_type, attribute_value)
                if isinstance(attr, ListAttribute) and attr.element_type and LIST in attribute_value:
                    if issubclass(attr.element_type, AttributeContainer):
                        for element in attribute_value[LIST]:
                            if MAP in element:
                                attr.element_type._update_attribute_types(element[MAP])
                    else:
                        for element in attribute_value[LIST]:
                            AttributeContainer._coerce_attribute_type(attr.element_type.attr_type, element)
                if isinstance(attr, AttributeContainer) and MAP in attribute_value:
                    attr._update_attribute_types(attribute_value[MAP])

    @staticmethod
    def _coerce_attribute_type(attr_type: str, attribute_value: Dict[str, Any]):
        # coerce attribute types to disambiguate json string and array types
        if attr_type == BINARY and STRING in attribute_value:
            attribute_value[BINARY] = attribute_value.pop(STRING)
        if attr_type in {BINARY_SET, NUMBER_SET, STRING_SET} and LIST in attribute_value:
            json_type = NUMBER if attr_type == NUMBER_SET else STRING
            if all(next(iter(v)) == json_type for v in attribute_value[LIST]):
                attribute_value[attr_type] = [value[json_type] for value in attribute_value.pop(LIST)]

    @classmethod
    def _get_discriminator_class(cls, attribute_values: Dict[str, Dict[str, Any]]) -> Optional[Type]:
        discriminator_attr = cls._get_discriminator_attribute()
        if discriminator_attr:
            discriminator_attribute_value = attribute_values.get(discriminator_attr.attr_name, None)
            if discriminator_attribute_value:
                discriminator_value = discriminator_attr.get_value(discriminator_attribute_value)
                return discriminator_attr.deserialize(discriminator_value)
        return None

    @classmethod
    def _instantiate(cls: Type[_ACT], attribute_values: Dict[str, Dict[str, Any]]) -> _ACT:
        stored_cls = cls._get_discriminator_class(attribute_values)
        if stored_cls and not issubclass(stored_cls, cls):
            raise ValueError("Cannot instantiate a {} from the returned class: {}".format(
                cls.__name__, stored_cls.__name__))
        instance = (stored_cls or cls)(_user_instantiated=False)
        AttributeContainer._container_deserialize(instance, attribute_values)
        return instance


class DiscriminatorAttribute(Attribute[type]):
    attr_type = STRING

    def __init__(self, attr_name: Optional[str] = None) -> None:
        super().__init__(attr_name=attr_name)
        self._class_map: Dict[type, Any] = {}
        self._discriminator_map: Dict[Any, type] = {}

    def register_class(self, cls: type, discriminator: Any):
        discriminator = discriminator(cls) if callable(discriminator) else discriminator
        current_class = self._discriminator_map.get(discriminator)
        if current_class and current_class != cls:
            raise ValueError("The discriminator value '{}' is already assigned to a class: {}".format(
                discriminator, current_class.__name__))

        if cls not in self._class_map:
            self._class_map[cls] = discriminator

        self._discriminator_map[discriminator] = cls

    def get_registered_subclasses(self, cls: type) -> List[type]:
        return [k for k in self._class_map.keys() if issubclass(k, cls)]

    def get_discriminator(self, cls: type) -> Optional[Any]:
        return self._class_map.get(cls)

    def __set__(self, instance: Any, value: Optional[type]) -> None:
        if type(instance) != value:
            raise ValueError("The discriminator attribute must be set to the instance type: {}".format(type(instance)))
        super().__set__(instance, value)

    def serialize(self, value):
        """
        Returns the discriminator value corresponding to the given class.
        """
        return self._class_map[value]

    def deserialize(self, value):
        """
        Returns the class corresponding to the given discriminator value.
        """
        if value not in self._discriminator_map:
            raise ValueError("Unknown discriminator value: {}".format(value))
        return self._discriminator_map[value]


class BinaryAttribute(Attribute[bytes]):
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
        Returns a decoded byte string from a base64 encoded value
        """
        return b64decode(value)


class BinarySetAttribute(Attribute[Set[bytes]]):
    """
    A binary set
    """
    attr_type = BINARY_SET
    null = True

    def serialize(self, value):
        """
        Returns a list of base64 encoded binary strings. Encodes empty sets as "None".
        """
        return [b64encode(v).decode(DEFAULT_ENCODING) for v in value] or None

    def deserialize(self, value):
        """
        Returns a set of decoded byte strings from base64 encoded values.
        """
        return {b64decode(v) for v in value}


class UnicodeAttribute(Attribute[str]):
    """
    A unicode attribute
    """
    attr_type = STRING


class UnicodeSetAttribute(Attribute[Set[str]]):
    """
    A unicode set
    """
    attr_type = STRING_SET
    null = True

    def serialize(self, value):
        """
        Returns a list of strings. Encodes empty sets as "None".
        """
        return list(value) or None

    def deserialize(self, value):
        """
        Returns a set from a list of strings.
        """
        return set(value)


class JSONAttribute(Attribute[Any]):
    """
    A JSON Attribute

    Encodes JSON to unicode internally
    """
    attr_type = STRING

    def serialize(self, value) -> Optional[str]:
        """
        Serializes JSON to unicode
        """
        if value is None:
            return None
        encoded = json.dumps(value)
        return encoded

    def deserialize(self, value):
        """
        Deserializes JSON
        """
        return json.loads(value, strict=False)


class BooleanAttribute(Attribute[bool]):
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


class NumberAttribute(Attribute[float]):
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


class NumberSetAttribute(Attribute[Set[float]]):
    """
    A number set attribute
    """
    attr_type = NUMBER_SET
    null = True

    def serialize(self, value):
        """
        Encodes a set of numbers as a JSON list. Encodes empty sets as "None".
        """
        return [json.dumps(v) for v in value] or None

    def deserialize(self, value):
        """
        Returns a set from a JSON list of numbers.
        """
        return {json.loads(v) for v in value}


class VersionAttribute(NumberAttribute):
    """
    A version attribute
    """
    null = True

    def __set__(self, instance, value):
        """
        Cast assigned value to int.
        """
        super().__set__(instance, int(value))

    def __get__(self, instance, owner):
        """
        Cast retrieved value to int.
        """
        val = super().__get__(instance, owner)
        return int(val) if isinstance(val, float) else val

    def serialize(self, value):
        """
        Cast value to int then encode as JSON
        """
        return super().serialize(int(value))

    def deserialize(self, value):
        """
        Decode numbers from JSON and cast to int.
        """
        return int(super().deserialize(value))


class TTLAttribute(Attribute[datetime]):
    """
    A time-to-live attribute that signifies when the item expires and can be automatically deleted.
    It can be assigned with a timezone-aware datetime value (for absolute expiry time)
    or a timedelta value (for expiry relative to the current time),
    but always reads as a UTC datetime value.
    """
    attr_type = NUMBER

    def _normalize(self, value):
        """
        Converts value to a UTC datetime
        """
        if value is None:
            return
        if isinstance(value, timedelta):
            value = int(time.time() + value.total_seconds())
        elif isinstance(value, datetime):
            if value.tzinfo is None:
                raise ValueError("datetime must be timezone-aware")
            value = calendar.timegm(value.utctimetuple())
        else:
            raise ValueError("TTLAttribute value must be a timedelta or datetime")
        return datetime.utcfromtimestamp(value).replace(tzinfo=timezone.utc)

    def __set__(self, instance, value):
        """
        Converts assigned values to a UTC datetime
        """
        super().__set__(instance, self._normalize(value))

    def serialize(self, value):
        """
        Serializes a datetime as a timestamp (Unix time).
        """
        if value is None:
            return None
        return json.dumps(calendar.timegm(self._normalize(value).utctimetuple()))

    def deserialize(self, value):
        """
        Deserializes a timestamp (Unix time) as a UTC datetime.
        """
        timestamp = json.loads(value)
        return datetime.utcfromtimestamp(timestamp).replace(tzinfo=timezone.utc)


class UTCDateTimeAttribute(Attribute[datetime]):
    """
    An attribute for storing a UTC Datetime
    """
    attr_type = STRING

    def serialize(self, value):
        """
        Takes a datetime object and returns a string
        """
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        # Padding of years under 1000 is inconsistent and depends on system strftime:
        # https://bugs.python.org/issue13305
        fmt = value.astimezone(timezone.utc).strftime(DATETIME_FORMAT).zfill(31)
        return fmt

    def deserialize(self, value):
        """
        Takes a UTC datetime string and returns a datetime object
        """
        return self._fast_parse_utc_date_string(value)

    @staticmethod
    def _fast_parse_utc_date_string(date_string: str) -> datetime:
        # Method to quickly parse strings formatted with '%Y-%m-%dT%H:%M:%S.%f+0000'.
        # This is ~5.8x faster than using strptime and 38x faster than dateutil.parser.parse.
        _int = int  # Hack to prevent global lookups of int, speeds up the function ~10%
        try:
            # Fix pre-1000 dates serialized on systems where strftime doesn't pad w/older PynamoDB versions.
            date_string = date_string.zfill(31)
            if (len(date_string) != 31 or date_string[4] != '-' or date_string[7] != '-'
                    or date_string[10] != 'T' or date_string[13] != ':' or date_string[16] != ':'
                    or date_string[19] != '.' or date_string[26:31] != '+0000'):
                raise ValueError("Datetime string '{}' does not match format '{}'".format(date_string, DATETIME_FORMAT))
            return datetime(
                _int(date_string[0:4]), _int(date_string[5:7]), _int(date_string[8:10]),
                _int(date_string[11:13]), _int(date_string[14:16]), _int(date_string[17:19]),
                _int(date_string[20:26]), timezone.utc
            )
        except (TypeError, ValueError):
            raise ValueError("Datetime string '{}' does not match format '{}'".format(date_string, DATETIME_FORMAT))


class NullAttribute(Attribute[None]):
    attr_type = NULL

    def serialize(self, value):
        return True

    def deserialize(self, value):
        return None


class MapAttribute(Attribute[Mapping[_KT, _VT]], AttributeContainer):
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

    So, how do we create this dichotomous behavior?
    All MapAttribute instances are initialized as AttributeContainers only. During construction of
    AttributeContainer classes (subclasses of MapAttribute and Model), any instances that are class attributes
    are transformed from AttributeContainers to Attributes (via the `_make_attribute` method call).
    """
    attr_type = MAP

    attribute_args = getfullargspec(Attribute.__init__).args[1:]

    def __init__(self, **attributes):
        # Store the kwargs used by Attribute.__init__ in case `_make_attribute` is called.
        self.attribute_kwargs = {arg: attributes.pop(arg) for arg in self.attribute_args if arg in attributes}

        # Assume all instances should behave like an AttributeContainer. Instances that are intended to be
        # used as Attributes will be transformed during creation of the containing class.
        # Because of this do not use MRO or cooperative multiple inheritance, call the parent class directly.
        AttributeContainer.__init__(self, **attributes)

        # It is possible that attributes names can collide with argument names of Attribute.__init__.
        # Assume that this is the case if any of the following are true:
        #   - the user passed in other attributes that did not match any argument names
        #   - this is a "raw" (i.e. non-subclassed) MapAttribute instance and attempting to store the attributes
        #     cannot raise a ValueError (if this assumption is wrong, calling `_make_attribute` removes them)
        #   - the names of all attributes in self.attribute_kwargs match attributes defined on the class
        if self.attribute_kwargs and (
                attributes or self.is_raw() or all(arg in self.get_attributes() for arg in self.attribute_kwargs)):
            self._set_attributes(**self.attribute_kwargs)

    def _is_attribute_container(self):
        # Determine if this instance is being used as an AttributeContainer or an Attribute.
        # AttributeContainer instances have an internal `attribute_values` dictionary that is removed
        # by the `_make_attribute` call during initialization of the containing class.
        return 'attribute_values' in self.__dict__

    def _make_attribute(self):
        # WARNING! This function is only intended to be called from the __set_name__ function.
        if not self._is_attribute_container():
            raise AssertionError("MapAttribute._make_attribute called on an initialized instance")
        # During initialization the kwargs were stored in `attribute_kwargs`. Remove them and re-initialize the class.
        kwargs = self.attribute_kwargs
        del self.attribute_kwargs
        del self.attribute_values
        Attribute.__init__(self, **kwargs)
        for name, attr in self.get_attributes().items():
            # Set a local attribute with the same name that shadows the class attribute.
            # Because attr is a data descriptor and the attribute already exists on the class,
            # we have to store the local copy directly into __dict__ to prevent calling attr.__set__.
            # Use deepcopy so that `attr_path` and any local attributes are also copied.
            self.__dict__[name] = deepcopy(attr)

    def _update_attribute_paths(self, path_segment):
        # WARNING! This function is only intended to be called from the __set_name__ function.
        if self._is_attribute_container():
            raise AssertionError("MapAttribute._update_attribute_paths called before MapAttribute._make_attribute")
        for name in self.get_attributes().keys():
            local_attr = self.__dict__[name]
            local_attr.attr_path.insert(0, path_segment)
            if isinstance(local_attr, MapAttribute):
                local_attr._update_attribute_paths(path_segment)

    def __eq__(self, other: Any) -> 'Comparison':  # type: ignore[override]
        if self._is_attribute_container():
            return self is other  # type: ignore
        return Attribute.__eq__(self, other)

    def __ne__(self, other: Any) -> 'Comparison':  # type: ignore[override]
        if self._is_attribute_container():
            return self is not other  # type: ignore
        return Attribute.__ne__(self, other)

    def __iter__(self):
        if self._is_attribute_container():
            return iter(self.attribute_values)
        return super().__iter__()

    def __getitem__(self, item: _KT) -> _VT:  # type: ignore
        if self._is_attribute_container():
            return self.attribute_values[item]
        # If this instance is being used as an Attribute, treat item access like the map dereference operator.
        # This provides equivalence between DynamoDB's nested attribute access for map elements (MyMap.nestedField)
        # and Python's item access for dictionaries (MyMap['nestedField']).
        if item in self.get_attributes():
            return getattr(self, item)
        elif self.is_raw():
            return Path(self.attr_path + [str(item)])  # type: ignore
        else:
            raise AttributeError("'{}' has no attribute '{}'".format(self.__class__.__name__, item))

    def __setitem__(self, item, value):
        if not self._is_attribute_container():
            raise TypeError("'{}' object does not support item assignment".format(self.__class__.__name__))
        if item in self.get_attributes():
            setattr(self, item, value)
        elif self.is_raw():
            self.attribute_values[item] = value
        else:
            raise AttributeError("'{}' has no attribute '{}'".format(self.__class__.__name__, item))

    def __getattr__(self, attr: str) -> _VT:
        # This should only be called for "raw" (i.e. non-subclassed) MapAttribute instances.
        # MapAttribute subclasses should access attributes via the Attribute descriptors.
        if self.is_raw() and self._is_attribute_container():
            try:
                return self.attribute_values[attr]
            except KeyError:
                pass
        raise AttributeError("'{}' has no attribute '{}'".format(self.__class__.__name__, attr))

    @overload  # type: ignore
    def __get__(self: _A, instance: None, owner: Any) -> _A: ...
    @overload
    def __get__(self: _MT, instance: Any, owner: Any) -> _MT: ...
    def __get__(self: _A, instance: Any, owner: Any) -> Union[_A, _T]:
        # just for typing
        return super().__get__(instance, owner)  # type: ignore

    def __setattr__(self, name, value):
        # "Raw" (i.e. non-subclassed) instances set their name-value pairs in the `attribute_values` dictionary.
        # MapAttribute subclasses should set attributes via the Attribute descriptors.
        if self.is_raw() and self._is_attribute_container():
            self.attribute_values[name] = value
        else:
            object.__setattr__(self, name, value)

    def __set__(self, instance: Any, value: Union[None, 'MapAttribute[_KT, _VT]', Mapping[_KT, _VT]]):
        if isinstance(value, collections.abc.Mapping):
            value = type(self)(**value)  # type: ignore
        return super().__set__(instance, value)  # type: ignore

    def __set_name__(self, owner: Type[Any], name: str) -> None:
        if issubclass(owner, AttributeContainer):
            # MapAttribute instances that are class attributes of an AttributeContainer class
            # should behave like an Attribute instance and not an AttributeContainer instance.
            self._make_attribute()

            super().__set_name__(owner, name)

            # To support creating expressions from nested attributes, MapAttribute instances
            # store local copies of the attributes in cls._attributes with `attr_path` set.
            # Prepend the `attr_path` lists with the dynamo attribute name.
            self._update_attribute_paths(self.attr_name)

    def _set_attributes(self, **attrs):
        """
        Sets the attributes for this object
        """
        if self.is_raw():
            for name, value in attrs.items():
                setattr(self, name, value)
        else:
            super()._set_attributes(**attrs)

    def is_correctly_typed(self, key, attr, *, null_check: bool = True):
        can_be_null = attr.null or not null_check
        value = getattr(self, key)
        if can_be_null and value is None:
            return True
        if getattr(self, key) is None:
            raise AttributeNullError(key)
        return True  # TODO: check that the actual type of `value` meets requirements of `attr`

    def validate(self, *, null_check: bool = False):
        return all(self.is_correctly_typed(k, v, null_check=null_check)
                   for k, v in self.get_attributes().items())

    def _serialize_undeclared_attributes(self, values, container: Dict):
        # Continue to serialize NULL values in "raw" map attributes for backwards compatibility.
        # This special case behavior for "raw" attributes should be removed in the future.
        for attr_name in values:
            if attr_name not in self.get_attributes():
                v = values[attr_name]
                attr_class = _get_class_for_serialize(v)
                attr_type = attr_class.attr_type
                attr_value = attr_class.serialize(v)
                if attr_value is None:
                    # When attribute values serialize to "None" (e.g. empty sets) we store {"NULL": True} in DynamoDB.
                    attr_type = NULL
                    attr_value = True
                container[attr_name] = {attr_type: attr_value}
        return container

    def serialize(self, values, *, null_check: bool = True):
        if not self.is_raw():
            # This is a subclassed MapAttribute that acts as an AttributeContainer.
            # Serialize the values based on the attributes in the class.

            if not isinstance(values, type(self)):
                # Copy the values onto an instance of the class for serialization.
                instance = type(self)()
                instance.attribute_values = {}  # clear any defaults
                for name in values:
                    if name in self.get_attributes():
                        setattr(instance, name, values[name])
                values = instance

            return AttributeContainer._container_serialize(values, null_check=null_check)

        # For a "raw" MapAttribute all fields are undeclared
        return self._serialize_undeclared_attributes(values, {})

    def deserialize(self, values):
        """
        Decode as a dict.
        """
        if not self.is_raw():
            # If this is a subclass of a MapAttribute (i.e typed), instantiate an instance
            return self._instantiate(values)

        return {
            k: DESERIALIZE_CLASS_MAP[attr_type].deserialize(attr_value)
            for k, v in values.items() for attr_type, attr_value in v.items()
        }

    @classmethod
    def is_raw(cls):
        return cls == MapAttribute

    def as_dict(self):
        result = {}
        for key, value in self.attribute_values.items():
            result[key] = value.as_dict() if isinstance(value, MapAttribute) else value
        return result


class DynamicMapAttribute(MapAttribute):
    """
    A map attribute that supports declaring attributes (like an AttributeContainer) but will also store
    any other values that are set on it (like a raw MapAttribute).

    >>> class MyDynamicMapAttribute(DynamicMapAttribute):
    >>>     a_date_time = UTCDateTimeAttribute()  # raw map attributes cannot serialize/deserialize datetime values
    >>>
    >>> dynamic_map = MyDynamicMapAttribute()
    >>> dynamic_map.a_date_time = datetime.utcnow()
    >>> dynamic_map.a_number = 5
    >>> dynamic_map.serialize()  # {'a_date_time': {'S': 'xxx'}, 'a_number': {'N': '5'}}
    """

    def __setattr__(self, name, value):
        # Set attributes via the Attribute descriptor if it exists.
        if name in self.get_attributes():
            object.__setattr__(self, name, value)
        else:
            super().__setattr__(name, value)

    def serialize(self, values, *, null_check: bool = True):
        if not isinstance(values, type(self)):
            # Copy the values onto an instance of the class for serialization.
            instance = type(self)()
            instance.attribute_values = {}  # clear any defaults
            instance._set_attributes(**values)
            values = instance

        # this serializes the class defined attributes.
        # we do this first because we have type checks that validate the data
        rval = AttributeContainer._container_serialize(values, null_check=null_check)

        # this serializes the dynamically defined attributes
        # we have no real type safety here so we have to dynamically construct the type to write to dynamo
        self._serialize_undeclared_attributes(values, rval)

        return rval

    def deserialize(self, values):
        # this deserializes the class defined attributes
        # we do this first so that the we populate the defined object attributes fields properly with type safety
        instance = self._instantiate(values)
        # this deserializes the dynamically defined attributes
        for attr_name, value in values.items():
            if instance._dynamo_to_python_attr(attr_name) not in instance.get_attributes():
                attr_type, attr_value = next(iter(value.items()))
                instance[attr_name] = DESERIALIZE_CLASS_MAP[attr_type].deserialize(attr_value)
        return instance

    @classmethod
    def is_raw(cls):
        # All subclasses of DynamicMapAttribute should be treated like "raw" map attributes.
        return True


def _get_class_for_serialize(value):
    if value is None:
        return NullAttribute()
    if isinstance(value, MapAttribute):
        return value
    value_type = type(value)
    if value_type not in SERIALIZE_CLASS_MAP:
        raise ValueError('Unknown value: {}'.format(value_type))
    return SERIALIZE_CLASS_MAP[value_type]


class ListAttribute(Generic[_T], Attribute[List[_T]]):
    attr_type = LIST
    element_type: Optional[Type[Attribute]] = None

    def __init__(
        self,
        hash_key: bool = False,
        range_key: bool = False,
        null: Optional[bool] = None,
        default: Optional[Union[Any, Callable[..., Any]]] = None,
        attr_name: Optional[str] = None,
        of: Optional[Type[_T]] = None,
    ) -> None:
        super().__init__(
            hash_key=hash_key,
            range_key=range_key,
            null=null,
            default=default,
            attr_name=attr_name,
        )
        if of:
            if not issubclass(of, Attribute):
                raise ValueError("'of' must be a subclass of Attribute")
            self.element_type = of

    def serialize(self, values):
        """
        Encode the given list of objects into a list of AttributeValue types.
        """
        rval = []
        for v in values:
            attr_class = self._get_serialize_class(v)
            if self.element_type and v is not None and not isinstance(attr_class, self.element_type):
                raise ValueError("List elements must be of type: {}".format(self.element_type.__name__))
            attr_type = attr_class.attr_type
            attr_value = attr_class.serialize(v)
            if attr_value is None:
                # When attribute values serialize to "None" (e.g. empty sets) we store {"NULL": True} in DynamoDB.
                attr_type = NULL
                attr_value = True
            rval.append({attr_type: attr_value})
        return rval

    def deserialize(self, values):
        """
        Decode from list of AttributeValue types.
        """
        if self.element_type:
            element_attr = self.element_type()
            if isinstance(element_attr, MapAttribute):
                element_attr._make_attribute()  # ensure attr_name exists
            deserialized_lst = []
            for idx, attribute_value in enumerate(values):
                value = None
                if NULL not in attribute_value:
                    # set attr_name in case `get_value` raises an exception
                    element_attr.attr_name = '{}[{}]'.format(self.attr_name, idx)
                    value = element_attr.deserialize(element_attr.get_value(attribute_value))
                deserialized_lst.append(value)
            return deserialized_lst

        return [
            DESERIALIZE_CLASS_MAP[attr_type].deserialize(attr_value)
            for v in values for attr_type, attr_value in v.items()
        ]

    def __getitem__(self, idx: int) -> Path:  # type: ignore
        if not isinstance(idx, int):
            raise TypeError("list indices must be integers, not {}".format(type(idx).__name__))

        if self.element_type:
            # If this instance is typed, return a properly configured attribute on list element access.
            element_attr = self.element_type()
            if isinstance(element_attr, MapAttribute):
                element_attr._make_attribute()
            element_attr.attr_path = list(self.attr_path)  # copy the document path before indexing last element
            element_attr.attr_name = '{}[{}]'.format(element_attr.attr_name, idx)
            if isinstance(element_attr, MapAttribute):
                for path_segment in reversed(element_attr.attr_path):
                    element_attr._update_attribute_paths(path_segment)
            return element_attr  # type: ignore

        return super().__getitem__(idx)

    def _get_serialize_class(self, value):
        if value is None:
            return NullAttribute()
        if isinstance(value, Attribute):
            return value
        if self.element_type:
            return self.element_type()
        return SERIALIZE_CLASS_MAP[type(value)]


DESERIALIZE_CLASS_MAP: Dict[str, Attribute] = {
    BINARY: BinaryAttribute(),
    BINARY_SET: BinarySetAttribute(),
    BOOLEAN: BooleanAttribute(),
    LIST: ListAttribute(),
    MAP: MapAttribute(),
    NULL: NullAttribute(),
    NUMBER: NumberAttribute(),
    NUMBER_SET: NumberSetAttribute(),
    STRING: UnicodeAttribute(),
    STRING_SET: UnicodeSetAttribute()
}

SERIALIZE_CLASS_MAP = {
    dict: MapAttribute(),
    list: ListAttribute(),
    set: ListAttribute(),
    bool: BooleanAttribute(),
    float: NumberAttribute(),
    int: NumberAttribute(),
    str: UnicodeAttribute(),
    bytes: BinaryAttribute(),
}
