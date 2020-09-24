from typing import Dict
from typing import List
from typing import Union

from pynamodb.attributes import Attribute
from pynamodb.expressions.operand import Path
from pynamodb.expressions.util import substitute_names


def create_projection_expression(attributes_to_get, placeholders: Dict[str, str]) -> str:
    if not isinstance(attributes_to_get, list):
        attributes_to_get = [attributes_to_get]
    expressions = [substitute_names(_get_document_path(attribute), placeholders) for attribute in attributes_to_get]
    return ', '.join(expressions)


def _get_document_path(attribute: Union[Attribute, Path, str]) -> List[str]:
    if isinstance(attribute, Attribute):
        return [attribute.attr_name]
    if isinstance(attribute, Path):
        return attribute.path
    return attribute.split('.')
