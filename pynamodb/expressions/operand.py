from pynamodb.constants import (
    ATTR_TYPE_MAP, BINARY_SET, LIST, MAP, NUMBER_SET, NUMBER_SHORT, SHORT_ATTR_TYPES, STRING_SET, STRING_SHORT
)
from pynamodb.expressions.condition import (
    BeginsWith, Between, Comparison, Contains, Exists, In, IsType, NotExists
)
from pynamodb.expressions.update import (
    AddAction, DeleteAction, RemoveAction, SetAction
)
from pynamodb.expressions.util import get_path_segments, get_value_placeholder, substitute_names


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
        from pynamodb.attributes import Attribute  # prevent circular import -- Attribute imports Path
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


class Value(_ConditionOperand):
    """
    Value is an operand that represents an attribute value.
    """
    format_string = '{0}'

    def __init__(self, value, attribute=None):
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in SHORT_ATTR_TYPES:
            (self.short_attr_type, value), = value.items()
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


class Path(_ConditionOperand):
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

    def __getitem__(self, idx):
        # list dereference operator
        if self.attribute and self.attribute.attr_type != LIST:
            raise TypeError("'{0}' object has no attribute __getitem__".format(self.attribute.__class__.__name__))
        if not isinstance(idx, int):
            raise TypeError("list indices must be integers, not {0}".format(type(idx).__name__))
        # The __getitem__ call returns a new Path instance without any attribute set.
        # This is intended since the list element is not the same attribute as the list itself.
        element_path = Path(self.path)  # copy the document path before indexing last element
        element_path.path[-1] = '{0}[{1}]'.format(self.path[-1], idx)
        return element_path

    def set(self, value):
        # Returns an update action that sets this attribute to the given value
        return SetAction(self, self._to_operand(value))

    def update(self, subset):
        # Returns an update action that adds the subset to this set attribute
        return AddAction(self, self._to_operand(subset))

    def difference_update(self, subset):
        # Returns an update action that deletes the subset from this set attribute
        return DeleteAction(self, self._to_operand(subset))

    def remove(self):
        # Returns an update action that removes this attribute from the item
        return RemoveAction(self)

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
