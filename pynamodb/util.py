"""
Utils
"""
import re


def snake_to_camel_case(var_name: str) -> str:
    """
    Converts camel case variable names to snake case variable_names
    """
    first_pass = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', var_name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', first_pass).lower()
