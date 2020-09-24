from typing import Dict
from typing import List
from typing import Optional
from typing import Sequence
from typing import TYPE_CHECKING

from pynamodb.constants import BINARY_SET
from pynamodb.constants import NUMBER
from pynamodb.constants import NUMBER_SET
from pynamodb.constants import STRING_SET

if TYPE_CHECKING:
    from pynamodb.expressions.operand import _Operand
    from pynamodb.expressions.operand import Path
    from pynamodb.expressions.operand import Value


class Action:
    format_string: str = ''

    def __init__(self, *values: '_Operand') -> None:
        self.values = values

    def serialize(self, placeholder_names: Dict[str, str], expression_attribute_values: Dict[str, str]) -> str:
        values = [value.serialize(placeholder_names, expression_attribute_values) for value in self.values]
        return self.format_string.format(*values)

    def __repr__(self) -> str:
        values = [str(value) for value in self.values]
        return self.format_string.format(*values)


class SetAction(Action):
    """
    The SET action adds an attribute to an item.
    """
    format_string = '{0} = {1}'

    def __init__(self, path: 'Path', value: '_Operand') -> None:
        super(SetAction, self).__init__(path, value)


class RemoveAction(Action):
    """
    The REMOVE action deletes an attribute from an item.
    """
    format_string = '{0}'

    def __init__(self, path: 'Path') -> None:
        super(RemoveAction, self).__init__(path)


class AddAction(Action):
    """
    The ADD action appends elements to a set or mathematically adds to a number attribute.
    """
    format_string = '{0} {1}'

    def __init__(self, path: 'Path', subset: 'Value') -> None:
        path._type_check(BINARY_SET, NUMBER, NUMBER_SET, STRING_SET)
        subset._type_check(BINARY_SET, NUMBER, NUMBER_SET, STRING_SET)
        super(AddAction, self).__init__(path, subset)


class DeleteAction(Action):
    """
    The DELETE action removes elements from a set.
    """
    format_string = '{0} {1}'

    def __init__(self, path: 'Path', subset: 'Value') -> None:
        path._type_check(BINARY_SET, NUMBER_SET, STRING_SET)
        subset._type_check(BINARY_SET, NUMBER_SET, STRING_SET)
        super(DeleteAction, self).__init__(path, subset)


class Update:

    def __init__(self, *actions: Action) -> None:
        self.set_actions: List[SetAction] = []
        self.remove_actions: List[RemoveAction] = []
        self.add_actions: List[AddAction] = []
        self.delete_actions: List[DeleteAction] = []
        for action in actions:
            self.add_action(action)

    def add_action(self, action: Action) -> None:
        if isinstance(action, SetAction):
            self.set_actions.append(action)
        elif isinstance(action, RemoveAction):
            self.remove_actions.append(action)
        elif isinstance(action, AddAction):
            self.add_actions.append(action)
        elif isinstance(action, DeleteAction):
            self.delete_actions.append(action)
        else:
            raise ValueError("unsupported action type: '{}'".format(action.__class__.__name__))

    def serialize(self, placeholder_names: Dict[str, str], expression_attribute_values: Dict[str, str]) -> Optional[str]:
        clauses = [
            self._get_clause('SET', self.set_actions, placeholder_names, expression_attribute_values),
            self._get_clause('REMOVE', self.remove_actions, placeholder_names, expression_attribute_values),
            self._get_clause('ADD', self.add_actions, placeholder_names, expression_attribute_values),
            self._get_clause('DELETE', self.delete_actions, placeholder_names, expression_attribute_values),
        ]
        expression = ' '.join(clause for clause in clauses if clause is not None)
        return expression or None

    @staticmethod
    def _get_clause(
            keyword: str,
            actions: Sequence[Action],
            placeholder_names: Dict[str, str],
            expression_attribute_values: Dict[str, str]
    ) -> Optional[str]:
        actions_string = ', '.join(
            action.serialize(placeholder_names, expression_attribute_values) for action in actions
        )
        return keyword + ' ' + actions_string if actions_string else None
