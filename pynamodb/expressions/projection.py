from pynamodb.attributes import Attribute
from pynamodb.expressions.condition import Path
from pynamodb.expressions.util import substitute_names


def create_projection_expression(attributes_to_get, placeholders):
    if not isinstance(attributes_to_get, list):
        attributes_to_get = [attributes_to_get]
    expression_split_pairs = [_get_expression_split_pair(attribute) for attribute in attributes_to_get]
    expressions = [substitute_names(expr, placeholders, split=split) for (expr, split) in expression_split_pairs]
    return ', '.join(expressions)


def _get_expression_split_pair(attribute):
    if isinstance(attribute, Attribute):
        return attribute.attr_name, False
    if isinstance(attribute, Path):
        return attribute.path, not attribute.attribute_name
    return attribute, True
