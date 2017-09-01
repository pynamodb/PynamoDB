from pynamodb.constants import (
    ATTR_TYPE_MAP, BINARY_SET, LIST, MAP, NUMBER_SET, NUMBER_SHORT, SHORT_ATTR_TYPES, STRING_SET
)
from pynamodb.expressions.condition import (
    BeginsWith, Between, Condition, Contains, Exists, In, IsType, NotExists
)
from pynamodb.expressions.update import (
    AddAction, DeleteAction, RemoveAction, SetAction
)
from pynamodb.expressions.util import get_path_segments


class Operand(object):
    """
    Operand is the base class for objects that support creating conditions from comparators.
    """

    def __eq__(self, other):
        return self._compare('=', other)

    def __ne__(self, other):
        return self._compare('<>', other)

    def __lt__(self, other):
        return self._compare('<', other)

    def __le__(self, other):
        return self._compare('<=', other)

    def __gt__(self, other):
        return self._compare('>', other)

    def __ge__(self, other):
        return self._compare('>=', other)

    def _compare(self, operator, other):
        return Condition(self, operator, self._serialize(other))

    def between(self, lower, upper):
        # This seemed preferable to other options such as merging value1 <= attribute & attribute <= value2
        # into one condition expression. DynamoDB only allows a single sort key comparison and having this
        # work but similar expressions like value1 <= attribute & attribute < value2 fail seems too brittle.
        return Between(self, self._serialize(lower), self._serialize(upper))

    def is_in(self, *values):
        values = [self._serialize(value) for value in values]
        return In(self, *values)

    def _serialize(self, value):
        # Check to see if value is already serialized
        if isinstance(value, dict) and len(value) == 1 and list(value.keys())[0] in SHORT_ATTR_TYPES:
            return value
        # Serialize value based on its type
        from pynamodb.attributes import _get_class_for_serialize
        attr_class = _get_class_for_serialize(value)
        return {ATTR_TYPE_MAP[attr_class.attr_type]: attr_class.serialize(value)}


class Path(Operand):
    """
    Path is an operand that represents either an attribute name or document path.
    In addition to supporting comparisons, Path also supports creating conditions from functions.
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
        return SetAction(self, value if isinstance(value, Path) else self._serialize(value))

    def update(self, subset):
        # Returns an update action that adds the subset to this set attribute
        return AddAction(self, self._serialize(subset))

    def difference_update(self, subset):
        # Returns an update action that deletes the subset from this set attribute
        return DeleteAction(self, self._serialize(subset))

    def remove(self):
        # Returns an update action that removes this attribute from the item
        return RemoveAction(self)

    def exists(self):
        return Exists(self)

    def does_not_exist(self):
        return NotExists(self)

    def is_type(self, attr_type):
        return IsType(self, attr_type)

    def startswith(self, prefix):
        # A 'pythonic' replacement for begins_with to match string behavior (e.g. "foo".startswith("f"))
        return BeginsWith(self, self._serialize(prefix))

    def contains(self, item):
        return Contains(self, self._serialize(item))

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
            return super(AttributePath, self)._serialize(value)
        return {ATTR_TYPE_MAP[self.attribute.attr_type]: self.attribute.serialize(value)}


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

    def _serialize(self, value):
        if not isinstance(value, int):
            raise TypeError("size must be compared to an integer, not {0}".format(type(value).__name__))
        return {NUMBER_SHORT: str(value)}

    def __str__(self):
        return "size({0})".format(self.path)

    def __repr__(self):
        return "Size({0})".format(repr(self.path))
