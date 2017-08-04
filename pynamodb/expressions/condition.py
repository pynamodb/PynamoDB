from pynamodb.expressions.util import get_value_placeholder, substitute_names


class Path(object):

    def __init__(self, path):
        self.path = path

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

    def _compare(self, operator, value):
        return Condition(self.path, operator, value)

    def between(self, value1, value2):
        # This seemed preferable to other options such as merging value1 <= attribute & attribute <= value2
        # into one condition expression. DynamoDB only allows a single sort key comparison and having this
        # work but similar expressions like value1 <= attribute & attribute < value2 fail seems too brittle.
        return Between(self.path, value1, value2)

    def startswith(self, prefix):
        # A 'pythonic' replacement for begins_with to match string behavior (e.g. "foo".startswith("f"))
        return BeginsWith(self.path, prefix)


class Condition(object):
    format_string = '{path} {operator} {0}'

    def __init__(self, path, operator, *values):
        self.path = path
        self.operator = operator
        self.values = values
        self.logical_operator = None
        self.other_condition = None

    def serialize(self, placeholder_names, expression_attribute_values):
        path = substitute_names(self.path, placeholder_names)
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
        self.logical_operator = 'AND'
        self.other_condition = other
        return self

    def __nonzero__(self):
        # Prevent users from accidentally comparing the condition object instead of the attribute instance
        raise TypeError("unsupported operand type(s) for bool: '{0}'".format(self.__class__.__name__))

    def __bool__(self):
        # Prevent users from accidentally comparing the condition object instead of the attribute instance
        raise TypeError("unsupported operand type(s) for bool: {0}".format(self.__class__.__name__))


class Between(Condition):
    format_string = '{path} {operator} {0} AND {1}'

    def __init__(self, attribute, value1, value2):
        super(Between, self).__init__(attribute, 'BETWEEN', value1, value2)


class BeginsWith(Condition):
    format_string = '{operator} ({path}, {0})'

    def __init__(self, attribute, prefix):
        super(BeginsWith, self).__init__(attribute, 'begins_with', prefix)
