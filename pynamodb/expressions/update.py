from pynamodb.constants import BINARY_SET_SHORT, LIST_SHORT, NUMBER_SET_SHORT, NUMBER_SHORT, STRING_SET_SHORT
from pynamodb.expressions.util import get_value_placeholder, substitute_names


class Action(object):
    format_string = ''

    def __init__(self, path, value=None):
        self.path = path
        self.value = value

    def serialize(self, placeholder_names, expression_attribute_values):
        path = substitute_names(self.path.split('.'), placeholder_names)
        value = get_value_placeholder(self.value, expression_attribute_values) if self.value else None
        return self.format_string.format(value, path=path)


class SetAction(Action):
    """
    The SET action adds an attribute to an item.
    """
    format_string = '{path} = {0}'

    def __init__(self, path, value):
        super(SetAction, self).__init__(path, value)


class IncrementAction(SetAction):
    """
    A SET action that is used to add to a number attribute.
    """
    format_string = '{path} = {path} + {0}'

    def __init__(self, path, amount):
        (attr_type, value), = amount.items()
        if attr_type != NUMBER_SHORT:
            raise ValueError("{0} must be a number".format(value))
        super(IncrementAction, self).__init__(path, amount)


class DecrementAction(SetAction):
    """
    A SET action that is used to subtract from a number attribute.
    """
    format_string = '{path} = {path} - {0}'

    def __init__(self, path, amount):
        (attr_type, value), = amount.items()
        if attr_type != NUMBER_SHORT:
            raise ValueError("{0} must be a number".format(value))
        super(DecrementAction, self).__init__(path, amount)


class AppendAction(SetAction):
    """
    A SET action that appends elements to the end of a list.
    """
    format_string = '{path} = list_append({path}, {0})'

    def __init__(self, path, elements):
        (attr_type, value), = elements.items()
        if attr_type != LIST_SHORT:
            raise ValueError("{0} must be a list".format(value))
        super(AppendAction, self).__init__(path, elements)


class PrependAction(SetAction):
    """
    A SET action that prepends elements to the beginning of a list.
    """
    format_string = '{path} = list_append({0}, {path})'

    def __init__(self, path, elements):
        (attr_type, value), = elements.items()
        if attr_type != LIST_SHORT:
            raise ValueError("{0} must be a list".format(value))
        super(PrependAction, self).__init__(path, elements)


class SetIfNotExistsAction(SetAction):
    """
    A SET action that avoids overwriting an existing attribute.
    """
    format_string = '{path} = if_not_exists({path}, {0})'


class RemoveAction(Action):
    """
    The REMOVE action deletes an attribute from an item.
    """
    format_string = '{path}'

    def __init__(self, path):
        super(RemoveAction, self).__init__(path)


class AddAction(Action):
    """
    The ADD action appends elements to a set or mathematically adds to a number attribute.
    """
    format_string = '{path} {0}'

    def __init__(self, path, subset):
        (attr_type, value), = subset.items()
        if attr_type not in [BINARY_SET_SHORT, NUMBER_SET_SHORT, NUMBER_SHORT, STRING_SET_SHORT]:
            raise ValueError("{0} must be a number or set".format(value))
        super(AddAction, self).__init__(path, subset)


class DeleteAction(Action):
    """
    The DELETE action removes elements from a set.
    """
    format_string = '{path} {0}'

    def __init__(self, path, subset):
        (attr_type, value), = subset.items()
        if attr_type not in [BINARY_SET_SHORT, NUMBER_SET_SHORT, STRING_SET_SHORT]:
            raise ValueError("{0} must be a set".format(value))
        super(DeleteAction, self).__init__(path, subset)


class Update(object):

    def __init__(self):
        self.set_actions = []
        self.remove_actions = []
        self.add_actions = []
        self.delete_actions = []

    def add_action(self, action):
        if isinstance(action, SetAction):
            self.set_actions.append(action)
        elif isinstance(action, RemoveAction):
            self.remove_actions.append(action)
        elif isinstance(action, AddAction):
            self.add_actions.append(action)
        elif isinstance(action, DeleteAction):
            self.delete_actions.append(action)
        else:
            raise ValueError("unsupported action type: '{0}'".format(action.__class__.__name__))

    def serialize(self, placeholder_names, expression_attribute_values):
        expression = None
        expression = self._add_clause(expression, 'SET', self.set_actions, placeholder_names, expression_attribute_values)
        expression = self._add_clause(expression, 'REMOVE', self.remove_actions, placeholder_names, expression_attribute_values)
        expression = self._add_clause(expression, 'ADD', self.add_actions, placeholder_names, expression_attribute_values)
        expression = self._add_clause(expression, 'DELETE', self.delete_actions, placeholder_names, expression_attribute_values)
        return expression

    @staticmethod
    def _add_clause(expression, keyword, actions, placeholder_names, expression_attribute_values):
        clause = Update._get_clause(keyword, actions, placeholder_names, expression_attribute_values)
        if clause is None:
            return expression
        return clause if expression is None else expression + " " + clause

    @staticmethod
    def _get_clause(keyword, actions, placeholder_names, expression_attribute_values):
        actions = ", ".join([action.serialize(placeholder_names, expression_attribute_values) for action in actions])
        return keyword + " " + actions if actions else None
