from pynamodb.constants import (
    ATTR_TYPE_MAP, BINARY_SET, LIST, LIST_SHORT, MAP, MAP_SHORT,
    NUMBER_SET, NUMBER_SHORT, SHORT_ATTR_TYPES, STRING_SET, STRING_SHORT
)
from pynamodb.expressions.condition import (
    BeginsWith, Between, Comparison, Contains, Exists, In, IsType, NotExists
)
from pynamodb.expressions.update import (
    AddAction, DeleteAction, RemoveAction, SetAction
)
from pynamodb.expressions.util import get_path_segments, get_value_placeholder, substitute_names
from six import string_types


class _Operand(object):
    """
    Operand is the base class for objects that can be operands in Condition and Update Expressions.
    """
    format_string = ''
    short_attr_type = None

    def __init__(self, *values):
        self.values = values

    def __repr__(self):
        return self.format_string.format(*self.values)

    def serialize(self, placeholder_names, expression_attribute_values):
        values = [self._serialize_value(value, placeholder_names, expression_attribute_values) for value in self.values]
        return self.format_string.format(*values)

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return value.serialize(placeholder_names, expression_attribute_values)

    def _to_operand(self, value):
        if isinstance(value, _Operand):
            return value
        from pynamodb.attributes import Attribute, MapAttribute  # prevent circular import -- Attribute imports Path
        if isinstance(value, MapAttribute) and value._is_attribute_container():
            return self._to_value(value)
        return Path(value) if isinstance(value, Attribute) else self._to_value(value)

    def _to_value(self, value):
        return Value(value)

    def _type_check(self, *types):
        if self.short_attr_type and self.short_attr_type not in types:
            raise ValueError("The data type of '{0}' must be one of {1}".format(self, list(types)))


class _ConditionOperand(_Operand):
    """
    A base class for Operands that can be used in Condition Expression comparisons.
    """

    def __eq__(self, other):
        return Comparison('=', self, self._to_operand(other))

    def __ne__(self, other):
        return Comparison('<>', self, self._to_operand(other))

    def __lt__(self, other):
        return Comparison('<', self, self._to_operand(other))

    def __le__(self, other):
        return Comparison('<=', self, self._to_operand(other))

    def __gt__(self, other):
        return Comparison('>', self, self._to_operand(other))

    def __ge__(self, other):
        return Comparison('>=', self, self._to_operand(other))

    def between(self, lower, upper):
        return Between(self, self._to_operand(lower), self._to_operand(upper))

    def is_in(self, *values):
        values = [self._to_operand(value) for value in values]
        return In(self, *values)


class _NumericOperand(_Operand):
    """
    A base class for Operands that can be used in the increment and decrement SET update actions.
    """

    def __add__(self, other):
        return _Increment(self, self._to_operand(other))

    def __radd__(self, other):
        return _Increment(self._to_operand(other), self)

    def __sub__(self, other):
        return _Decrement(self, self._to_operand(other))

    def __rsub__(self, other):
        return _Decrement(self._to_operand(other), self)


class _ListAppendOperand(_Operand):
    """
    A base class for Operands that can be used in the list_append function for the SET update action.
    """

    def append(self, other):
        return _ListAppend(self, self._to_operand(other))

    def prepend(self, other):
        return _ListAppend(self._to_operand(other), self)


class _Size(_ConditionOperand):
    """
    Size is a special operand that represents the result of calling the 'size' function on a Path operand.
    """
    format_string = 'size ({0})'
    short_attr_type = NUMBER_SHORT

    def __init__(self, path):
        if not isinstance(path, Path):
            path = Path(path)
        super(_Size, self).__init__(path)

    def _to_operand(self, value):
        operand = super(_Size, self)._to_operand(value)
        operand._type_check(NUMBER_SHORT)
        return operand


class _Increment(_Operand):
    """
    Increment is a special operand that represents an increment SET update action.
    """
    format_string = '{0} + {1}'
    short_attr_type = NUMBER_SHORT

    def __init__(self, lhs, rhs):
        lhs._type_check(NUMBER_SHORT)
        rhs._type_check(NUMBER_SHORT)
        super(_Increment, self).__init__(lhs, rhs)


class _Decrement(_Operand):
    """
    Decrement is a special operand that represents an decrement SET update action.
    """
    format_string = '{0} - {1}'
    short_attr_type = NUMBER_SHORT

    def __init__(self, lhs, rhs):
        lhs._type_check(NUMBER_SHORT)
        rhs._type_check(NUMBER_SHORT)
        super(_Decrement, self).__init__(lhs, rhs)


class _ListAppend(_Operand):
    """
    ListAppend is a special operand that represents the list_append function for the SET update action.
    """
    format_string = 'list_append ({0}, {1})'
    short_attr_type = LIST_SHORT

    def __init__(self, list1, list2):
        list1._type_check(LIST_SHORT)
        list2._type_check(LIST_SHORT)
        super(_ListAppend, self).__init__(list1, list2)


class _IfNotExists(_NumericOperand, _ListAppendOperand):
    """
    IfNotExists is a special operand that represents the if_not_exists function for the SET update action.
    """
    format_string = 'if_not_exists ({0}, {1})'

    def __init__(self, path, value):
        self.short_attr_type = path.short_attr_type or value.short_attr_type
        if self.short_attr_type != value.short_attr_type:
            # path and value have conflicting types -- defer any type checks to DynamoDB
            self.short_attr_type = None
        super(_IfNotExists, self).__init__(path, value)


class Value(_NumericOperand, _ListAppendOperand, _ConditionOperand):
    """
    Value is an operand that represents an attribute value.
    """
    format_string = '{0}'

    def __init__(self, value, attribute=None):
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in SHORT_ATTR_TYPES:
            (self.short_attr_type, value), = value.items()
        elif value is None:
            (self.short_attr_type, value) = Value.__serialize(value)
        else:
            (self.short_attr_type, value) = Value.__serialize(value, attribute)
        super(Value, self).__init__({self.short_attr_type: value})

    @property
    def value(self):
        return self.values[0]

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return get_value_placeholder(value, expression_attribute_values)

    @staticmethod
    def __serialize(value, attribute=None):
        if attribute is None:
            return Value.__serialize_based_on_type(value)
        if attribute.attr_type == LIST and not isinstance(value, list):
            # List attributes assume the values to be serialized are lists.
            (attr_type, attr_value), = attribute.serialize([value])[0].items()
            return attr_type, attr_value
        if attribute.attr_type == MAP and not isinstance(value, dict):
            # Map attributes assume the values to be serialized are maps.
            return Value.__serialize_based_on_type(value)
        return ATTR_TYPE_MAP[attribute.attr_type], attribute.serialize(value)

    @staticmethod
    def __serialize_based_on_type(value):
        from pynamodb.attributes import _get_class_for_serialize
        attr_class = _get_class_for_serialize(value)
        return ATTR_TYPE_MAP[attr_class.attr_type], attr_class.serialize(value)


class Path(_NumericOperand, _ListAppendOperand, _ConditionOperand):
    """
    Path is an operand that represents either an attribute name or document path.
    """
    format_string = '{0}'

    def __init__(self, attribute_or_path):
        from pynamodb.attributes import Attribute  # prevent circular import -- Attribute imports Path
        is_attribute = isinstance(attribute_or_path, Attribute)
        self.attribute = attribute_or_path if is_attribute else None
        self.short_attr_type = ATTR_TYPE_MAP[attribute_or_path.attr_type] if is_attribute else None
        path = attribute_or_path.attr_path if is_attribute else attribute_or_path
        if not path:
            raise ValueError("path cannot be empty")
        super(Path, self).__init__(get_path_segments(path))

    @property
    def path(self):
        return self.values[0]

    def __iter__(self):
        # Because we define __getitem__ Path is considered an iterable
        raise TypeError("'{0}' object is not iterable".format(self.__class__.__name__))

    def __getitem__(self, item):
        # The __getitem__ call returns a new Path instance without any attribute set.
        # This is intended since the nested element is not the same attribute as ``self``.
        if self.attribute and self.attribute.attr_type not in [LIST, MAP]:
            raise TypeError("'{0}' object has no attribute __getitem__".format(self.attribute.__class__.__name__))
        if self.short_attr_type == LIST_SHORT and not isinstance(item, int):
            raise TypeError("list indices must be integers, not {0}".format(type(item).__name__))
        if self.short_attr_type == MAP_SHORT and not isinstance(item, string_types):
            raise TypeError("map attributes must be strings, not {0}".format(type(item).__name__))
        if isinstance(item, int):
            # list dereference operator
            element_path = Path(self.path)  # copy the document path before indexing last element
            element_path.path[-1] = '{0}[{1}]'.format(self.path[-1], item)
            return element_path
        if isinstance(item, string_types):
            # map dereference operator
            return Path(self.path + [item])
        raise TypeError("item must be an integer or string, not {0}".format(type(item).__name__))

    def __or__(self, other):
        return _IfNotExists(self, self._to_operand(other))

    def set(self, value):
        # Returns an update action that sets this attribute to the given value
        return SetAction(self, self._to_operand(value))

    def remove(self):
        # Returns an update action that removes this attribute from the item
        return RemoveAction(self)

    def add(self, *values):
        # Returns an update action that appends the given values to a set or mathematically adds a value to a number
        value = values[0] if len(values) == 1 else values
        return AddAction(self, self._to_operand(value))

    def delete(self, *values):
        # Returns an update action that removes the given values from a set attribute
        value = values[0] if len(values) == 1 else values
        return DeleteAction(self, self._to_operand(value))

    def exists(self):
        return Exists(self)

    def does_not_exist(self):
        return NotExists(self)

    def is_type(self, attr_type):
        if attr_type not in SHORT_ATTR_TYPES:
            raise ValueError("{0} is not a valid attribute type. Must be one of {1}".format(
                attr_type, SHORT_ATTR_TYPES))
        return IsType(self, Value(attr_type))

    def startswith(self, prefix):
        # A 'pythonic' replacement for begins_with to match string behavior (e.g. "foo".startswith("f"))
        operand = self._to_operand(prefix)
        operand._type_check(STRING_SHORT)
        return BeginsWith(self, operand)

    def contains(self, item):
        if self.attribute and self.attribute.attr_type in [BINARY_SET, NUMBER_SET, STRING_SET]:
            # Set attributes assume the values to be serialized are sets.
            (attr_type, attr_value), = self._to_value([item]).value.items()
            item = {attr_type[0]: attr_value[0]}
        return Contains(self, self._to_operand(item))

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return substitute_names(value, placeholder_names)

    def _to_value(self, value):
        return Value(value, attribute=self.attribute)

    def __str__(self):
        # Quote the path to illustrate that any dot characters are not dereference operators.
        quoted_path = [self._quote_path(segment) if '.' in segment else segment for segment in self.path]
        return '.'.join(quoted_path)

    def __repr__(self):
        return "Path({0})".format(self.path)

    @staticmethod
    def _quote_path(path):
        path, sep, rem = path.partition('[')
        return repr(path) + sep + rem
