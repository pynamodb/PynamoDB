from typing import Dict, List, Optional, Union

from pynamodb.attributes import Attribute
from pynamodb.expressions.condition import (
    BeginsWith, Between, Comparison, Contains, Exists, In, IsType, NotExists
)
from pynamodb.expressions.update import (
    AddAction, DeleteAction, RemoveAction, SetAction
)


class _Operand(object):
    format_string: str
    short_attr_type: Optional[str]

    values: List[Path]

    def __init__(self, *values: Path) -> None: ...
    def serialize(self, placeholder_names: Dict[str, str], expression_attribute_values: Dict[str, str]) -> str: ...


class _NumericOperand(_Operand): ...


class _ListAppendOperand(_Operand): ...


class _ConditionOperand(_Operand): ...


class _Size(_ConditionOperand):
    def __init__(self, path: Union[Path, Attribute]) -> None: ...


class _Increment(_Operand):
    def __init__(self, lhs: Union[Path, Attribute]) -> None: ...


class _Decrement(_Operand):
    def __init__(self, lhs: Union[Path, Attribute]) -> None: ...


class Value(_NumericOperand, _ListAppendOperand, _ConditionOperand):
    def __init__(self, value: Optional[Union[Path, Dict]], attribute=Optional[Attribute]) -> None: ...


class Path(_NumericOperand, _ListAppendOperand, _ConditionOperand):
    attribute: Union[Attribute, Path]
    short_attr_type: Optional[str]

    def __init__(self, attribute_or_path: Union[Attribute, Path, str]) -> None: ...
    @property
    def path(self) -> Path: ...
    def delete(self, *values: Path) -> DeleteAction: ...
    def exists(self) -> Exists: ...
    def does_not_exist(self) -> NotExists: ...
    def is_type(self, attr_type: str) -> IsType: ...

