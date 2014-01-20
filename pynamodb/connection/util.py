"""
Utils
"""
import re


def pythonic(var_name):
    """
    Converts CamelCase variable names to pythonic variable_names
    """
    first_pass = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', var_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', first_pass).lower()
