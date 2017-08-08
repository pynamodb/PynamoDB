from copy import copy
from pynamodb.constants import AND, BETWEEN
from pynamodb.expressions.util import get_value_placeholder, substitute_names


class Path(object):

    def __init__(self, path, attribute_name=False):
        self.path = path
        self.attribute_name = attribute_name

    def __getitem__(self, idx):
        # list dereference operator
        if not isinstance(idx, int):
            raise TypeError("list indices must be integers, not {0}".format(type(idx).__name__))
        element_path = copy(self)
        element_path.path = '{0}[{1}]'.format(self.path, idx)
        return element_path

    def __eq__(self, other):
        return self._compare('=', other)

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

    def startswith(self, prefix):
        # A 'pythonic' replacement for begins_with to match string behavior (e.g. "foo".startswith("f"))
        return BeginsWith(self, self._serialize(prefix))

    def _serialize(self, value):
        # Allow subclasses to define value serialization.
        return value

    def __str__(self):
        if self.attribute_name and '.' in self.path:
            # Quote the path to illustrate that the dot characters are not dereference operators.
            path, sep, rem = self.path.partition('[')
            return repr(path) + sep + rem
        return self.path

    def __repr__(self):
        return "Path('{0}', attribute_name={1})".format(self.path, self.attribute_name)


class Condition(object):
    format_string = '{path} {operator} {0}'

    def __init__(self, path, operator, *values):
        self.path = path
        self.operator = operator
        self.values = values
        self.logical_operator = None
        self.other_condition = None

    def serialize(self, placeholder_names, expression_attribute_values):
        split = not self.path.attribute_name
        path = substitute_names(self.path.path, placeholder_names, split=split)
        values = [get_value_placeholder(value, expression_attribute_values) for value in self.values]
        condition = self.format_string.format(*values, path=path, operator=self.operator)
        if self.logical_operator:
            other_condition = self.other_condition.serialize(placeholder_names, expression_attribute_values)
            return '{0} {1} {2}'.format(condition, self.logical_operator, other_condition)
        return condition

    def __and__(self, other):
        if not isinstance(other, Condition):
            raise TypeError("unsupported operand type(s) for &: '{0}' and '{1}'",
                            self.__class__.__name__, other.__class__.__name__)
        self.logical_operator = AND
        self.other_condition = other
        return self

    def __repr__(self):
        values = [value.items()[0][1] for value in self.values]
        condition = self.format_string.format(*values, path=self.path, operator = self.operator)
        if self.logical_operator:
            other_conditions = repr(self.other_condition)
            return '{0} {1} {2}'.format(condition, self.logical_operator, other_conditions)
        return condition

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


class BeginsWith(Condition):
    format_string = '{operator} ({path}, {0})'

    def __init__(self, path, prefix):
        super(BeginsWith, self).__init__(path, 'begins_with', prefix)
