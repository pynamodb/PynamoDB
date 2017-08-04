from pynamodb.expressions.util import substitute_names


def create_projection_expression(attributes_to_get, placeholders):
    expressions = [substitute_names(attribute, placeholders) for attribute in attributes_to_get]
    return ', '.join(expressions)
