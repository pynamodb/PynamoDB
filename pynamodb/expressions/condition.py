from pynamodb.constants import (
    AND, BETWEEN, BINARY_SHORT, IN, NUMBER_SHORT, OR, SHORT_ATTR_TYPES, STRING_SHORT
)
from pynamodb.expressions.util import get_value_placeholder, substitute_names
from six.moves import range


# match dynamo function syntax: size(path)
def size(path):
    from pynamodb.expressions.operand import Size
    return Size(path)


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
        from pynamodb.expressions.operand import Path, Size
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
