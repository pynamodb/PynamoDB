from typing import Any, Dict, List, Optional, Union
from typing import TYPE_CHECKING

from pynamodb.constants import (
    ATTRIBUTE_TYPES, BINARY_SET, LIST, MAP, NUMBER, NUMBER_SET, STRING, STRING_SET
)
from pynamodb.expressions.condition import (
    BeginsWith, Between, Comparison, Contains, Exists, In, IsType, NotExists
)
from pynamodb.expressions.update import (
    AddAction, DeleteAction, RemoveAction, SetAction
)
from pynamodb.expressions.util import get_path_segments, get_value_placeholder, substitute_names

if TYPE_CHECKING:
    from pynamodb.attributes import Attribute


class _Operand:
    """
    Operand is the base class for objects that can be operands in Condition and Update Expressions.
    """
    format_string = ''
    attr_type: Optional[str] = None

    def __init__(self, *values: Any) -> None:
        self.values = values

    def __repr__(self) -> str:
        return self.format_string.format(*self.values)

    def serialize(self, placeholder_names: Dict[str, str], expression_attribute_values: Dict[str, str]) -> str:
        values = [self._serialize_value(value, placeholder_names, expression_attribute_values) for value in self.values]
        return self.format_string.format(*values)

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return value.serialize(placeholder_names, expression_attribute_values)

    def _to_operand(self, value: Union['_Operand', 'Attribute', Any]):
        if isinstance(value, _Operand):
            return value
        from pynamodb.attributes import Attribute, MapAttribute  # prevent circular import -- Attribute imports Path
        if isinstance(value, MapAttribute) and value._is_attribute_container():
            return self._to_value(value)
        return Path(value) if isinstance(value, Attribute) else self._to_value(value)

    def _to_value(self, value):
        return Value(value)

    def _type_check(self, *types):
        if self.attr_type and self.attr_type not in types:
            raise ValueError("The data type of '{}' must be one of {}".format(self, list(types)))


class _ConditionOperand(_Operand):
    """
    A base class for Operands that can be used in Condition Expression comparisons.
    """

    def __eq__(self, other: Any) -> Comparison:  # type: ignore
        return Comparison('=', self, self._to_operand(other))

    def __ne__(self, other: Any) -> Comparison:  # type: ignore
        return Comparison('<>', self, self._to_operand(other))

    def __lt__(self, other: Any) -> Comparison:
        return Comparison('<', self, self._to_operand(other))

    def __le__(self, other: Any) -> Comparison:
        return Comparison('<=', self, self._to_operand(other))

    def __gt__(self, other: Any) -> Comparison:
        return Comparison('>', self, self._to_operand(other))

    def __ge__(self, other: Any) -> Comparison:
        return Comparison('>=', self, self._to_operand(other))

    def between(self, lower: Any, upper: Any) -> Between:
        return Between(self, self._to_operand(lower), self._to_operand(upper))

    def is_in(self, *values: Any) -> In:
        op_values = [self._to_operand(value) for value in values]
        return In(self, *op_values)


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

    def append(self, other: Any) -> '_ListAppend':
        return _ListAppend(self, self._to_operand(other))

    def prepend(self, other: Any) -> '_ListAppend':
        return _ListAppend(self._to_operand(other), self)


class _Size(_ConditionOperand):
    """
    Size is a special operand that represents the result of calling the 'size' function on a Path operand.
    """
    format_string = 'size ({0})'
    attr_type = NUMBER

    def __init__(self, path: Union['Path', 'Attribute', str, List[str]]) -> None:
        if not isinstance(path, Path):
            path = Path(path)
        super(_Size, self).__init__(path)

    def _to_operand(self, value):
        operand = super(_Size, self)._to_operand(value)
        operand._type_check(NUMBER)
        return operand


class _Increment(_Operand):
    """
    Increment is a special operand that represents an increment SET update action.
    """
    format_string = '{0} + {1}'
    attr_type = NUMBER

    def __init__(self, lhs: '_Operand', rhs: '_Operand') -> None:
        lhs._type_check(NUMBER)
        rhs._type_check(NUMBER)
        super(_Increment, self).__init__(lhs, rhs)


class _Decrement(_Operand):
    """
    Decrement is a special operand that represents an decrement SET update action.
    """
    format_string = '{0} - {1}'
    attr_type = NUMBER

    def __init__(self, lhs: _Operand, rhs: _Operand) -> None:
        lhs._type_check(NUMBER)
        rhs._type_check(NUMBER)
        super(_Decrement, self).__init__(lhs, rhs)


class _ListAppend(_Operand):
    """
    ListAppend is a special operand that represents the list_append function for the SET update action.
    """
    format_string = 'list_append ({0}, {1})'
    attr_type = LIST

    def __init__(self, list1: _Operand, list2: _Operand):
        list1._type_check(LIST)
        list2._type_check(LIST)
        super(_ListAppend, self).__init__(list1, list2)


class _IfNotExists(_NumericOperand, _ListAppendOperand):
    """
    IfNotExists is a special operand that represents the if_not_exists function for the SET update action.
    """
    format_string = 'if_not_exists ({0}, {1})'

    def __init__(self, path: _Operand, value: Any) -> None:
        self.attr_type = path.attr_type or value.attr_type
        if self.attr_type != value.attr_type:
            # path and value have conflicting types -- defer any type checks to DynamoDB
            self.attr_type = None
        super(_IfNotExists, self).__init__(path, value)


class Value(_NumericOperand, _ListAppendOperand, _ConditionOperand):
    """
    Value is an operand that represents an attribute value.
    """
    format_string = '{0}'

    def __init__(self, value: Any, attribute: Optional['Attribute'] = None) -> None:
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in ATTRIBUTE_TYPES:
            (self.attr_type, value), = value.items()
        elif value is None:
            (self.attr_type, value) = Value.__serialize(value)
        else:
            (self.attr_type, value) = Value.__serialize(value, attribute)
        super(Value, self).__init__({self.attr_type: value})

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
        return attribute.attr_type, attribute.serialize(value)

    @staticmethod
    def __serialize_based_on_type(value):
        from pynamodb.attributes import _get_class_for_serialize
        attr_class = _get_class_for_serialize(value)
        return attr_class.attr_type, attr_class.serialize(value)


class Path(_NumericOperand, _ListAppendOperand, _ConditionOperand):
    """
    Path is an operand that represents either an attribute name or document path.
    """
    format_string = '{0}'

    def __init__(self, attribute_or_path: Union['Attribute', str, List[str]]) -> None:
        from pynamodb.attributes import Attribute  # prevent circular import -- Attribute imports Path
        path: Union[str, List[str]]
        if isinstance(attribute_or_path, Attribute):
            self.attribute = attribute_or_path
            self.attr_type = attribute_or_path.attr_type
            path = attribute_or_path.attr_path
        else:
            self.attribute = None
            self.attr_type = None
            path = attribute_or_path
        if not path:
            raise ValueError("path cannot be empty")
        super(Path, self).__init__(get_path_segments(path))

    @property
    def path(self) -> List[str]:
        return self.values[0]

    def __iter__(self):
        # Because we define __getitem__ Path is considered an iterable
        raise TypeError("'{}' object is not iterable".format(self.__class__.__name__))

    def __getitem__(self, item: Union[int, str]) -> 'Path':
        # The __getitem__ call returns a new Path instance without any attribute set.
        # This is intended since the nested element is not the same attribute as ``self``.
        if self.attribute and self.attribute.attr_type not in [LIST, MAP]:
            raise TypeError("'{}' object has no attribute __getitem__".format(self.attribute.__class__.__name__))
        if self.attr_type == LIST and not isinstance(item, int):
            raise TypeError("list indices must be integers, not {}".format(type(item).__name__))
        if self.attr_type == MAP and not isinstance(item, str):
            raise TypeError("map attributes must be strings, not {}".format(type(item).__name__))
        if isinstance(item, int):
            # list dereference operator
            element_path = Path(self.path)  # copy the document path before indexing last element
            element_path.path[-1] = '{}[{}]'.format(self.path[-1], item)
            return element_path
        if isinstance(item, str):
            # map dereference operator
            return Path(self.path + [item])
        raise TypeError("item must be an integer or string, not {}".format(type(item).__name__))

    def __or__(self, other):
        return _IfNotExists(self, self._to_operand(other))

    def set(self, value: Any) -> SetAction:
        # Returns an update action that sets this attribute to the given value
        return SetAction(self, self._to_operand(value))

    def remove(self) -> RemoveAction:
        # Returns an update action that removes this attribute from the item
        return RemoveAction(self)

    def add(self, *values: Any) -> AddAction:
        # Returns an update action that appends the given values to a set or mathematically adds a value to a number
        value = values[0] if len(values) == 1 else values
        return AddAction(self, self._to_operand(value))

    def delete(self, *values: Any) -> DeleteAction:
        # Returns an update action that removes the given values from a set attribute
        value = values[0] if len(values) == 1 else values
        return DeleteAction(self, self._to_operand(value))

    def exists(self) -> Exists:
        return Exists(self)

    def does_not_exist(self) -> NotExists:
        return NotExists(self)

    def is_type(self, attr_type: str) -> IsType:
        if attr_type not in ATTRIBUTE_TYPES:
            raise ValueError("{} is not a valid attribute type. Must be one of {}".format(
                attr_type, ATTRIBUTE_TYPES))
        return IsType(self, Value(attr_type))

    def startswith(self, prefix: str) -> BeginsWith:
        # A 'pythonic' replacement for begins_with to match string behavior (e.g. "foo".startswith("f"))
        operand = self._to_operand(prefix)
        operand._type_check(STRING)
        return BeginsWith(self, operand)

    def contains(self, item: Any) -> Contains:
        if self.attribute and self.attribute.attr_type in [BINARY_SET, NUMBER_SET, STRING_SET]:
            # Set attributes assume the values to be serialized are sets.
            (attr_type, attr_value), = self._to_value([item]).value.items()
            item = {attr_type[0]: attr_value[0]}
        return Contains(self, self._to_operand(item))

    def _serialize_value(self, value, placeholder_names, expression_attribute_values):
        return substitute_names(value, placeholder_names)

    def _to_value(self, value: Any) -> Value:
        return Value(value, attribute=self.attribute)

    def __str__(self) -> str:
        # Quote the path to illustrate that any dot characters are not dereference operators.
        quoted_path = [self._quote_path(segment) if '.' in segment else segment for segment in self.path]
        return '.'.join(quoted_path)

    def __repr__(self) -> str:
        return "Path({})".format(self.path)

    @staticmethod
    def _quote_path(path: str) -> str:
        path, sep, rem = path.partition('[')
        return repr(path) + sep + rem
