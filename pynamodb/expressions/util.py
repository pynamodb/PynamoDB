import re

PATH_SEGMENT_REGEX = re.compile(r'([^\[\]]+)((?:\[\d+\])*)$')


def substitute_names(document_path, placeholders):
    """
    Replaces all attribute names in the given document path with placeholders.
    Stores the placeholders in the given dictionary.
    """
    for idx, segment in enumerate(document_path):
        match = PATH_SEGMENT_REGEX.match(segment)
        if not match:
            raise ValueError('{0} is not a valid document path'.format('.'.join(document_path)))
        name, indexes = match.groups()
        if name in placeholders:
            placeholder = placeholders[name]
        else:
            placeholder = '#' + str(len(placeholders))
            placeholders[name] = placeholder
        document_path[idx] = placeholder + indexes
    return '.'.join(document_path)


def get_value_placeholder(value, expression_attribute_values):
    placeholder = ':' + str(len(expression_attribute_values))
    expression_attribute_values[placeholder] = value
    return placeholder
