from pynamodb.constants import BINARY_SET_SHORT, NUMBER_SET_SHORT, NUMBER_SHORT, STRING_SET_SHORT


class Action(object):
    format_string = ''

    def __init__(self, *values):
        self.values = values

    def serialize(self, placeholder_names, expression_attribute_values):
        values = [value.serialize(placeholder_names, expression_attribute_values) for value in self.values]
        return self.format_string.format(*values)

    def __repr__(self):
        values = [str(value) for value in self.values]
        return self.format_string.format(*values)


class SetAction(Action):
    """
    The SET action adds an attribute to an item.
    """
    format_string = '{0} = {1}'

    def __init__(self, path, value):
        super(SetAction, self).__init__(path, value)


class RemoveAction(Action):
    """
    The REMOVE action deletes an attribute from an item.
    """
    format_string = '{0}'

    def __init__(self, path):
        super(RemoveAction, self).__init__(path)


class AddAction(Action):
    """
    The ADD action appends elements to a set or mathematically adds to a number attribute.
    """
    format_string = '{0} {1}'

    def __init__(self, path, subset):
        subset._type_check(BINARY_SET_SHORT, NUMBER_SET_SHORT, NUMBER_SHORT, STRING_SET_SHORT)
        super(AddAction, self).__init__(path, subset)


class DeleteAction(Action):
    """
    The DELETE action removes elements from a set.
    """
    format_string = '{0} {1}'

    def __init__(self, path, subset):
        subset._type_check(BINARY_SET_SHORT, NUMBER_SET_SHORT, STRING_SET_SHORT)
        super(DeleteAction, self).__init__(path, subset)


class Update(object):

    def __init__(self, *actions):
        self.set_actions = []
        self.remove_actions = []
        self.add_actions = []
        self.delete_actions = []
        for action in actions:
            self.add_action(action)

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
