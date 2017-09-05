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


class Operand(object):
    """
    Operand is the base class for objects that can be operands in Condition and Update Expressions.
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

    def serialize(self, placeholder_names, expression_attribute_values):
        raise NotImplementedError('serialize has not been implemented for {0}'.format(self.__class__.__name__))

    def _has_type(self, short_type):
        raise NotImplementedError('_has_type has not been implemented for {0}'.format(self.__class__.__name__))

    def _to_operand(self, value):
        from pynamodb.attributes import Attribute  # prevent circular import -- Attribute imports AttributePath
        if isinstance(value, Attribute):
            return AttributePath(value)
        return value if isinstance(value, Operand) else self._to_value(value)

    def _to_value(self, value):
        return Value(value)


class Value(Operand):
    """
    Value is an operand that represents an attribute value.
    """

    def __init__(self, value, attribute=None):
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in SHORT_ATTR_TYPES:
            self.value = value
        else:
            self.value = Value._serialize_value(value, attribute)

    def serialize(self, placeholder_names, expression_attribute_values):
        return get_value_placeholder(self.value, expression_attribute_values)

    def _has_type(self, short_type):
        (attr_type, value), = self.value.items()
        return short_type == attr_type

    def __str__(self):
        (attr_type, value), = self.value.items()
        try:
            from pynamodb.attributes import _get_class_for_deserialize
            attr_class = _get_class_for_deserialize(self.value)
            return str(attr_class.deserialize(value))
        except ValueError:
            return str(value)

    def __repr__(self):
        return "Value({0})".format(self.value)

    @staticmethod
    def _serialize_value(value, attribute=None):
        if attribute is None:
            return Value._serialize_value_based_on_type(value)
        if attribute.attr_type == LIST and not isinstance(value, list):
            # List attributes assume the values to be serialized are lists.
            return attribute.serialize([value])[0]
        if attribute.attr_type == MAP and not isinstance(value, dict):
            # Map attributes assume the values to be serialized are maps.
            return Value._serialize_value_based_on_type(value)
        return {ATTR_TYPE_MAP[attribute.attr_type]: attribute.serialize(value)}

    @staticmethod
    def _serialize_value_based_on_type(value):
        from pynamodb.attributes import _get_class_for_serialize
        attr_class = _get_class_for_serialize(value)
        return {ATTR_TYPE_MAP[attr_class.attr_type]: attr_class.serialize(value)}


class Path(Operand):
    """
    Path is an operand that represents either an attribute name or document path.
    """

    def __init__(self, path):
        if not path:
            raise ValueError("path cannot be empty")
        self.path = get_path_segments(path)

    def __getitem__(self, idx):
        # list dereference operator
        if not isinstance(idx, int):
            raise TypeError("list indices must be integers, not {0}".format(type(idx).__name__))
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
        if not operand._has_type(STRING_SHORT):
            raise ValueError("{0} must be a string operand".format(operand))
        return BeginsWith(self, operand)

    def contains(self, item):
        return Contains(self, self._to_operand(item))

    def serialize(self, placeholder_names, expression_attribute_values):
        return substitute_names(self.path, placeholder_names)

    def _has_type(self, short_type):
        # Assume the attribute has the correct type
        return True

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


class AttributePath(Path):

    def __init__(self, attribute):
        super(AttributePath, self).__init__(attribute.attr_path)
        self.attribute = attribute

    def __getitem__(self, idx):
        if self.attribute.attr_type != LIST:  # only list elements support the list dereference operator
            raise TypeError("'{0}' object has no attribute __getitem__".format(self.attribute.__class__.__name__))
        # The __getitem__ call returns a new Path instance, not an AttributePath instance.
        # This is intended since the list element is not the same attribute as the list itself.
        return super(AttributePath, self).__getitem__(idx)

    def contains(self, item):
        if self.attribute.attr_type in [BINARY_SET, NUMBER_SET, STRING_SET]:
            # Set attributes assume the values to be serialized are sets.
            (attr_type, attr_value), = self._to_value([item]).value.items()
            item = {attr_type[0]: attr_value[0]}
        return super(AttributePath, self).contains(item)

    def _has_type(self, short_type):
        return ATTR_TYPE_MAP[self.attribute.attr_type] == short_type

    def _to_value(self, value):
        return Value(value, attribute=self.attribute)


class Size(Operand):
    """
    Size is a special operand that represents the result of calling the 'size' function on a Path operand.
    """

    def __init__(self, path):
        # prevent circular import -- Attribute imports AttributePath
        from pynamodb.attributes import Attribute
        if isinstance(path, Path):
            self.path = path
        elif isinstance(path, Attribute):
            self.path = AttributePath(path)
        else:
            self.path = Path(path)

    def _to_operand(self, value):
        operand = super(Size, self)._to_operand(value)
        if not operand._has_type(NUMBER_SHORT):
            raise ValueError("size must be compared to a number, not {0}".format(operand))
        return operand

    def serialize(self, placeholder_names, expression_attribute_values):
        return "size ({0})".format(substitute_names(self.path.path, placeholder_names))

    def __str__(self):
        return "size({0})".format(self.path)

    def __repr__(self):
        return "Size({0})".format(repr(self.path))
