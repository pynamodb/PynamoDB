from typing import Dict, List

from pynamodb.expressions.operand import Path


class Action:
    def __init__(self, *values: Path) -> None: ...


class SetAction(Action):
    def __init__(self, path: Path, value: Path) -> None: ...


class RemoveAction(Action):
    def __init__(self, path: Path) -> None: ...


class AddAction(Action):
    def __init__(self, path: Path, subset: Path) -> None: ...


class DeleteAction(Action):
    def __init__(self, path: Path, subset: Path) -> None: ...


class Update(object):
    set_actions: List[SetAction]
    remove_actions: List[RemoveAction]
    add_actions: List[AddAction]
    delete_actions: List[DeleteAction]
    def __init__(self, *actions: Action) -> None: ...
    def add_action(self, action: Action): ...
    def serialize(self, placeholder_names: Dict[str, str], expression_attribute_values: Dict[str, str]): ...
