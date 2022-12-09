import json
from base64 import b64decode
from base64 import b64encode
from typing import Any
from typing import Dict

from pynamodb.constants import BINARY
from pynamodb.constants import BINARY_SET
from pynamodb.constants import BOOLEAN
from pynamodb.constants import LIST
from pynamodb.constants import MAP
from pynamodb.constants import NULL
from pynamodb.constants import NUMBER
from pynamodb.constants import NUMBER_SET
from pynamodb.constants import STRING
from pynamodb.constants import STRING_SET


def attr_value_to_simple_dict(attribute_value: Dict[str, Any], force: bool) -> Any:
    attr_type, attr_value = next(iter(attribute_value.items()))
    if attr_type == LIST:
        return [attr_value_to_simple_dict(v, force) for v in attr_value]
    if attr_type == MAP:
        return {k: attr_value_to_simple_dict(v, force) for k, v in attr_value.items()}
    if attr_type == NULL:
        return None
    if attr_type == BOOLEAN:
        return attr_value
    if attr_type == STRING:
        return attr_value
    if attr_type == NUMBER:
        return json.loads(attr_value)
    if attr_type == BINARY:
        if force:
            return b64encode(attr_value).decode()
        raise ValueError("Binary attributes are not supported")
    if attr_type == BINARY_SET:
        if force:
            return [b64encode(v).decode() for v in attr_value]
        raise ValueError("Binary set attributes are not supported")
    if attr_type == STRING_SET:
        if force:
            return attr_value
        raise ValueError("String set attributes are not supported")
    if attr_type == NUMBER_SET:
        if force:
            return [json.loads(v) for v in attr_value]
        raise ValueError("Number set attributes are not supported")
    raise ValueError("Unknown attribute type: {}".format(attr_type))


def simple_dict_to_attr_value(value: Any) -> Dict[str, Any]:
    if value is None:
        return {NULL: True}
    if value is True or value is False:
        return {BOOLEAN: value}
    if isinstance(value, (int, float)):
        return {NUMBER: json.dumps(value)}
    if isinstance(value, str):
        return {STRING: value}
    if isinstance(value, list):
        return {LIST: [simple_dict_to_attr_value(v) for v in value]}
    if isinstance(value, dict):
        return {MAP: {k: simple_dict_to_attr_value(v) for k, v in value.items()}}
    raise ValueError("Unknown value type: {}".format(type(value).__name__))


def _b64encode(b: bytes) -> str:
    return b64encode(b).decode()


def bin_encode_attr(attr: Dict[str, Any]) -> None:
    if BINARY in attr:
        attr[BINARY] = _b64encode(attr[BINARY])
    elif BINARY_SET in attr:
        attr[BINARY_SET] = [_b64encode(v) for v in attr[BINARY_SET]]
    elif MAP in attr:
        for sub_attr in attr[MAP].values():
            bin_encode_attr(sub_attr)
    elif LIST in attr:
        for sub_attr in attr[LIST]:
            bin_encode_attr(sub_attr)


def bin_decode_attr(attr: Dict[str, Any]) -> None:
    if BINARY in attr:
        attr[BINARY] = b64decode(attr[BINARY])
    elif BINARY_SET in attr:
        attr[BINARY_SET] = [b64decode(v) for v in attr[BINARY_SET]]
    elif MAP in attr:
        for sub_attr in attr[MAP].values():
            bin_decode_attr(sub_attr)
    elif LIST in attr:
        for sub_attr in attr[LIST]:
            bin_decode_attr(sub_attr)
