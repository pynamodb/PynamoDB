from typing import Dict


# match dynamo function syntax: size(path)
def size(path):
    from pynamodb.expressions.operand import _Size
    return _Size(path)


class Condition(object):
    format_string: str = ''

    def __init__(self, operator: str, *values) -> None:
        self.operator = operator
        self.values = values

    def serialize(self, placeholder_names: Dict[str, str], expression_attribute_values: Dict[str, str]) -> str:
        values = [value.serialize(placeholder_names, expression_attribute_values) for value in self.values]
        return self.format_string.format(*values, operator=self.operator)

    def __and__(self, other):
        if not isinstance(other, Condition):
            raise TypeError("unsupported operand type(s) for &: '{}' and '{}'"
                            .format(self.__class__.__name__, other.__class__.__name__))
        return And(self, other)

    def __rand__(self, other):
        # special case 'None & condition' to enable better syntax for chaining
        if other is not None:
            raise TypeError("unsupported operand type(s) for &: '{}' and '{}'"
                            .format(other.__class__.__name__, self.__class__.__name__))
        return self

    def __or__(self, other):
        if not isinstance(other, Condition):
            raise TypeError("unsupported operand type(s) for |: '{}' and '{}'"
                            .format(self.__class__.__name__, other.__class__.__name__))
        return Or(self, other)

    def __invert__(self):
        return Not(self)

    def __repr__(self) -> str:
        values = [str(value) for value in self.values]
        return self.format_string.format(*values, operator=self.operator)

    def __bool__(self):
        # Prevent users from accidentally comparing the condition object instead of the attribute instance
        raise TypeError("unsupported operand type(s) for bool: {}".format(self.__class__.__name__))


class Comparison(Condition):
    format_string = '{0} {operator} {1}'

    def __init__(self, operator, lhs, rhs):
        if operator not in ['=', '<>', '<', '<=', '>', '>=']:
            raise ValueError("{0} is not a valid comparison operator: {0}".format(operator))
        super().__init__(operator, lhs, rhs)


class Between(Condition):
    format_string = '{0} {operator} {1} AND {2}'

    def __init__(self, path, lower, upper):
        super().__init__('BETWEEN', path, lower, upper)


class In(Condition):
    def __init__(self, path, *values):
        super().__init__('IN', path, *values)
        list_format = ', '.join('{' + str(i + 1) + '}' for i in range(len(values)))
        self.format_string = '{0} {operator} (' + list_format + ')'


class Exists(Condition):
    format_string = '{operator} ({0})'

    def __init__(self, path):
        super().__init__('attribute_exists', path)


class NotExists(Condition):
    format_string = '{operator} ({0})'

    def __init__(self, path):
        super().__init__('attribute_not_exists', path)


class IsType(Condition):
    format_string = '{operator} ({0}, {1})'

    def __init__(self, path, attr_type):
        super().__init__('attribute_type', path, attr_type)


class BeginsWith(Condition):
    format_string = '{operator} ({0}, {1})'

    def __init__(self, path, prefix):
        super().__init__('begins_with', path, prefix)


class Contains(Condition):
    format_string = '{operator} ({0}, {1})'

    def __init__(self, path, operand):
        super().__init__('contains', path, operand)


class And(Condition):
    format_string = '({0} {operator} {1})'

    def __init__(self, condition1: Condition, condition2: Condition) -> None:
        super().__init__('AND', condition1, condition2)


class Or(Condition):
    format_string = '({0} {operator} {1})'

    def __init__(self, condition1: Condition, condition2: Condition) -> None:
        super().__init__('OR', condition1, condition2)


class Not(Condition):
    format_string = '({operator} {0})'

    def __init__(self, condition: Condition) -> None:
        super().__init__('NOT', condition)
