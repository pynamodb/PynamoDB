PATH_SEGMENT_REGEX = re.compile(r'([^\[\]]+)((?:\[\d+\])*)$')

class Expression(object):
    """Immutable"""

    """
    TODO between, IN, and NOT
    http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.OperatorsAndFunctions.html
    """

    def __init__(self, raw=''):
        self.raw = raw

    def __eq__(self, other):
        return Expression('{} = {}'.format(self.raw, other.raw))

    def __lt__(self, other):
        return Expression('{} < {}'.format(self.raw, other.raw))

    def __le__(self, other):
        return Expression('{} <= {}'.format(self.raw, other.raw))

    def __gt__(self, other):
        return Expression('{} > {}'.format(self.raw, other.raw))

    def __ge__(self, other):
        return Expression('{} >= {}'.format(self.raw, other.raw))

    def __or__(self, other):
        return Expression('{} OR {}'.format(self.raw, other.raw))

    def __and__(self, other):
        return Expression('{} AND {}'.format(self.raw, other.raw))

    def __ne__(self, other):
        return Expression('{} <> {}'.format(self.raw, other.raw))


def substitute_names(expression, placeholders):
    """
    Replaces names in the given expression with placeholders.
    Stores the placeholders in the given dictionary.
    """
    return _substitute(expression, placeholders, '#')


def substitute_values(expression, placeholders):
    return _substitute(expression, placeholders, ':')


def _substitute(expression, placeholders, identifier):
    path_segments = expression.split('.')
    for idx, segment in enumerate(path_segments):
        match = PATH_SEGMENT_REGEX.match(segment)
        if not match:
            raise ValueError('{0} is not a valid document path'.format(expression))
        name, indexes = match.groups()
        if name in placeholders:
            placeholder = placeholders[name]
        else:
            placeholder = identifier + str(len(placeholders))
            placeholders[name] = placeholder
        path_segments[idx] = placeholder + indexes
    return '.'.join(path_segments)
