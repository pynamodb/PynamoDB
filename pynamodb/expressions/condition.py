from pynamodb.constants import (
    AND, ATTR_TYPE_MAP, BETWEEN, BINARY_SHORT, IN, NUMBER_SHORT, OR, SHORT_ATTR_TYPES, STRING_SHORT
)
from pynamodb.expressions.util import get_value_placeholder, substitute_names
from six import string_types
from six.moves import range


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


class Size(Operand):
    """
    Size is a special operand that represents the result of calling the 'size' function on a Path operand.
    """

    def __init__(self, path):
        # prevent circular import -- AttributePath imports Path
        from pynamodb.attributes import Attribute, AttributePath
        if isinstance(path, Path):
            self.path = Path
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


# match dynamo function syntax: size(path)
def size(path):
    return Size(path)


class Path(Operand):
    """
    Path is an operand that represents either an attribute name or document path.
    In addition to supporting comparisons, Path also supports creating conditions from functions.
    """

    def __init__(self, path):
        if not path:
            raise ValueError("path cannot be empty")
        self.path = path.split('.') if isinstance(path, string_types) else list(path)

    def __getitem__(self, idx):
        # list dereference operator
        if not isinstance(idx, int):
            raise TypeError("list indices must be integers, not {0}".format(type(idx).__name__))
        element_path = Path(self.path)  # copy the document path before indexing last element
        element_path.path[-1] = '{0}[{1}]'.format(self.path[-1], idx)
        return element_path

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


class Condition(object):
    format_string = '{path} {operator} {0}'

    def __init__(self, path, operator, *values):
        self.path = path
        self.operator = operator
        self.values = values

    # http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_Query.html#DDB-Query-request-KeyConditionExpression
    def is_valid_range_key_condition(self, path):
        return str(self.path) == path and self.operator in ['=', '<', '<=', '>', '>=', BETWEEN, 'begins_with']

    def serialize(self, placeholder_names, expression_attribute_values):
        path = self._get_path(self.path, placeholder_names)
        values = self._get_values(placeholder_names, expression_attribute_values)
        return self.format_string.format(*values, path=path, operator=self.operator)

    def _get_path(self, path, placeholder_names):
        if isinstance(path, Path):
            return substitute_names(path.path, placeholder_names)
        elif isinstance(path, Size):
            return "size ({0})".format(self._get_path(path.path, placeholder_names))
        else:
            return path

    def _get_values(self, placeholder_names, expression_attribute_values):
        return [
            value.serialize(placeholder_names, expression_attribute_values)
            if isinstance(value, Condition)
            else get_value_placeholder(value, expression_attribute_values)
            for value in self.values
        ]

    def __and__(self, other):
        if not isinstance(other, Condition):
            raise TypeError("unsupported operand type(s) for &: '{0}' and '{1}'",
                            self.__class__.__name__, other.__class__.__name__)
        return And(self, other)

    def __or__(self, other):
        if not isinstance(other, Condition):
            raise TypeError("unsupported operand type(s) for |: '{0}' and '{1}'",
                            self.__class__.__name__, other.__class__.__name__)
        return Or(self, other)

    def __invert__(self):
        return Not(self)

    def __repr__(self):
        values = [repr(value) if isinstance(value, Condition) else list(value.items())[0][1] for value in self.values]
        return self.format_string.format(*values, path=self.path, operator = self.operator)

    def __nonzero__(self):
        # Prevent users from accidentally comparing the condition object instead of the attribute instance
        raise TypeError("unsupported operand type(s) for bool: '{0}'".format(self.__class__.__name__))

    def __bool__(self):
        # Prevent users from accidentally comparing the condition object instead of the attribute instance
        raise TypeError("unsupported operand type(s) for bool: {0}".format(self.__class__.__name__))


class Between(Condition):
    format_string = '{path} {operator} {0} AND {1}'

    def __init__(self, path, lower, upper):
        super(Between, self).__init__(path, BETWEEN, lower, upper)


class In(Condition):
    def __init__(self, path, *values):
        super(In, self).__init__(path, IN, *values)
        list_format = ', '.join('{' + str(i) + '}' for i in range(len(values)))
        self.format_string = '{path} {operator} (' + list_format + ')'


class Exists(Condition):
    format_string = '{operator} ({path})'

    def __init__(self, path):
        super(Exists, self).__init__(path, 'attribute_exists')


class NotExists(Condition):
    format_string = '{operator} ({path})'

    def __init__(self, path):
        super(NotExists, self).__init__(path, 'attribute_not_exists')


class IsType(Condition):
    format_string = '{operator} ({path}, {0})'

    def __init__(self, path, attr_type):
        if attr_type not in SHORT_ATTR_TYPES:
            raise ValueError("{0} is not a valid attribute type. Must be one of {1}".format(
                attr_type, SHORT_ATTR_TYPES))
        super(IsType, self).__init__(path, 'attribute_type', {STRING_SHORT: attr_type})


class BeginsWith(Condition):
    format_string = '{operator} ({path}, {0})'

    def __init__(self, path, prefix):
        super(BeginsWith, self).__init__(path, 'begins_with', prefix)


class Contains(Condition):
    format_string = '{operator} ({path}, {0})'

    def __init__(self, path, item):
        (attr_type, value), = item.items()
        if attr_type not in [BINARY_SHORT, NUMBER_SHORT, STRING_SHORT]:
            raise ValueError("{0} must be a string, number, or binary element".format(value))
        super(Contains, self).__init__(path, 'contains', item)


class And(Condition):
    format_string = '({0} {operator} {1})'

    def __init__(self, condition1, condition2):
        super(And, self).__init__(None, AND, condition1, condition2)


class Or(Condition):
    format_string = '({0} {operator} {1})'

    def __init__(self, condition1, condition2):
        super(Or, self).__init__(None, OR, condition1, condition2)


class Not(Condition):
    format_string = '({operator} {0})'

    def __init__(self, condition):
        super(Not, self).__init__(None, 'NOT', condition)
