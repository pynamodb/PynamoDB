from pynamodb.attributes import Attribute
from pynamodb.expressions.operand import Path
from pynamodb.expressions.util import substitute_names


def create_projection_expression(attributes_to_get, placeholders):
    if not isinstance(attributes_to_get, list):
        attributes_to_get = [attributes_to_get]
    expressions = [substitute_names(_get_document_path(attribute), placeholders) for attribute in attributes_to_get]
    return ', '.join(expressions)


def _get_document_path(attribute):
    if isinstance(attribute, Attribute):
        return [attribute.attr_name]
    if isinstance(attribute, Path):
        return attribute.path
    return attribute.split('.')
